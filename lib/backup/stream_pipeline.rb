module Backup
  ##
  # Runs a shell pipeline and hands its final STDOUT to the caller as an IO, so a dump can
  # be consumed as it is produced instead of being staged on disk first.
  #
  # This is the streaming counterpart to Pipeline. Pipeline collects the per-command exit
  # statuses on STDOUT and pushes the last command's output into a file, which is exactly
  # backwards for streaming: here STDOUT carries the data and everything else is routed to
  # its own pipe on a higher file descriptor.
  #
  # == Why every stream gets its own pipe and its own reader
  #
  # A child cannot exit while it is blocked writing to a pipe nobody is draining, and a
  # parent waiting for one pipe to reach EOF will wait forever if the child is blocked on a
  # different one. That is the bug that cost a night of production backups before
  # 5.0.0.beta.5 (see Utilities.run), with only two pipes in play. This class has more:
  # data, the exit-status report, the shell's own STDERR, and one STDERR per command.
  #
  # So the rule is absolute and not conditional on how much output a stage is expected to
  # produce: every pipe is read to EOF, concurrently, for the whole life of the child. The
  # data pipe is drained by the caller's block; each of the others gets a dedicated thread.
  # Only the amount of STDERR *retained in memory* is bounded -- never the amount read.
  #
  # Giving each command its own STDERR pipe also means a line can be attributed to the
  # command that wrote it, so messages are logged as "mongodump:STDERR: ..." exactly as
  # Utilities.run logs them. Logger#ignore_warning patterns written against that prefix
  # keep working.
  #
  #   pipeline = StreamPipeline.new
  #   pipeline << "mongodump --archive --db='foo'"
  #   pipeline << "pigz -c -5"
  #   pipeline.run { |io| upload(io) }
  #
  class StreamPipeline
    class Error < Backup::Error; end

    include Utilities::Helpers

    ##
    # Bytes of each command's STDERR retained for reporting.
    #
    # Reading is never limited -- see above -- but an hour-long dump emits megabytes of
    # progress lines and holding all of it helps nobody. The head keeps the "writing
    # <db>.<collection>" lines that say how far a dump got; the tail keeps whatever was
    # said just before a failure.
    STDERR_HEAD_BYTES = 64 * 1024
    STDERR_TAIL_BYTES = 192 * 1024

    ##
    # Bytes read from the data pipe per read(2). Only affects syscall count.
    DATA_READ_BYTES = 128 * 1024

    ##
    # Requested capacity for the data pipe, applied best-effort.
    #
    # A pipe holds 64 KiB by default on Linux. That is enough for correctness, but the
    # consumer of a stream spends most of its time doing something else (uploading a part),
    # and a deeper buffer lets the compressor keep working through those gaps instead of
    # blocking on a full pipe. Unsupported anywhere but Linux, and subject to
    # /proc/sys/fs/pipe-max-size, so failure to set it is ignored.
    DATA_PIPE_BYTES = 1024 * 1024
    F_SETPIPE_SZ = 1031

    ##
    # Seconds to wait for a terminated pipeline to die before sending KILL.
    TERMINATE_GRACE = 10

    ##
    # Seconds to wait for a drain thread to finish after the pipeline has been reaped.
    #
    # It should be immediate: a drain ends when every writer of its pipe is closed, and by
    # this point the parent has closed its copies and the child is gone. But "waits forever
    # with nothing in the log" is the exact failure this class exists to avoid, and it would
    # be absurd to reintroduce it in the teardown. Nothing is lost by giving up here -- the
    # pipeline has already finished -- so a stuck drain costs some STDERR, not the backup.
    DRAIN_JOIN_TIMEOUT = 30

    ##
    # First file descriptor used for a command's STDERR. 0-2 are the standard streams and 3
    # carries the exit-status report.
    FIRST_STDERR_FD = 4

    ##
    # STDERR collected from the run, as [label, text] pairs: the shell's own output first,
    # then one entry per command in pipeline order.
    #
    # An Array and not a Hash keyed by command name, because a pipeline may legitimately run
    # the same program twice and a Hash would silently drop one of them.
    attr_reader :errors, :stderr_messages

    def initialize
      @commands = []
      @success_codes = []
      @errors = []
      @stderr_messages = []
    end

    ##
    # Adds a command to the pipeline. +success_codes+ must be an Array of Integer exit
    # codes considered successful for that command.
    def add(command, success_codes)
      @commands << command
      @success_codes << success_codes
    end

    ##
    # Adds a command which is only successful with exit status 0.
    def <<(command)
      add(command, [0])
    end

    ##
    # Spawns the pipeline and yields the IO carrying the last command's STDOUT.
    #
    # The block is expected to read that IO to EOF. Once it returns, the pipeline is reaped
    # and every command's exit status is checked; an Error is raised if any command failed
    # or if any command failed to report a status at all.
    #
    # If the block raises, the entire process group is killed rather than left writing into
    # a pipe nobody is reading, and the block's exception propagates unchanged -- a failed
    # upload should not be reported as a failed dump.
    def run
      raise Error, "No commands to run" if @commands.empty?

      data_r, data_w = IO.pipe
      status_r, status_w = IO.pipe
      shell_err_r, shell_err_w = IO.pipe
      command_err = @commands.map { IO.pipe }
      writers = [data_w, status_w, shell_err_w, *command_err.map(&:last)]
      readers = [status_r, shell_err_r, *command_err.map(&:first)]

      widen_data_pipe(data_w)

      spawn_options = {
        # Not :close. A closed fd 0 is a trap: the next file a command opens lands on it.
        in: File::NULL,
        out: data_w,
        err: shell_err_w,
        3 => status_w,
        # Own process group, so a failure can take down every stage of the pipeline and not
        # just the shell that started them.
        pgroup: true
      }
      command_err.each_with_index do |(_r, w), index|
        spawn_options[FIRST_STDERR_FD + index] = w
      end

      pid = Process.spawn(pipeline, spawn_options)

      # Nothing reaches EOF while the parent still holds a writer open.
      writers.each(&:close)

      drains = [Thread.new { drain(shell_err_r) }]
      command_err.each { |(r, _w)| drains << Thread.new { drain(r) } }
      status_drain = Thread.new { status_r.read.to_s }

      block_error = nil
      begin
        yield data_r
      rescue Exception => err
        block_error = err
        terminate(pid)
      ensure
        data_r.close unless data_r.closed?
        reap(pid)
        # Safe only after the child is gone: these threads sit in read(2) until every
        # writer of their pipe has closed, which happens when the last stage exits.
        collected = drains.map { |thread| join_drain(thread) }
        status_report = join_drain(status_drain).to_s
        readers.each { |io| io.close unless io.closed? }
      end

      record_stderr(collected)
      log_stderr

      raise block_error if block_error

      check_statuses(status_report)
      raise Error, "Stream failed!\n#{error_messages}" unless success?

      true
    end

    def success?
      @errors.empty?
    end

    ##
    # A multi-line report of every command that failed, with whatever it said on STDERR.
    def error_messages
      report = @errors.map { |err| "#{err.class}: #{err.message}" }.join("\n")
      stderr = @stderr_messages.reject { |_label, text| text.empty? }.map do |label, text|
        "#{label}:STDERR:\n#{text}"
      end.join("\n")

      stderr.empty? ? report : "#{report}\n#{stderr}"
    end

    ##
    # STDERR from the command at +index+ in the pipeline.
    def stderr_for(index)
      pair = @stderr_messages[index + 1]
      pair && pair.last
    end

    private

    ##
    # The shell pipeline.
    #
    # Each command is grouped with an `echo` reporting its index and exit status on fd 3,
    # and has its STDERR sent to its own descriptor from FIRST_STDERR_FD up. STDOUT is left
    # alone: for every command but the last it is the next stage's STDIN, and for the last
    # it is the data the caller reads.
    #
    # Statuses may arrive in any order, which is why the index is sent along with them.
    def pipeline
      parts = @commands.each_with_index.map do |command, index|
        %({ #{command} 2>&#{FIRST_STDERR_FD + index} ; echo "#{index}|$?:" >&3 ; })
      end
      parts.join(" | ")
    end

    ##
    # Reads +io+ to EOF, retaining at most STDERR_HEAD_BYTES from the start and
    # STDERR_TAIL_BYTES from the end. Reading always continues to EOF regardless of how
    # much is kept, because the writer blocks otherwise.
    def drain(io)
      head = +""
      tail = +""
      dropped = 0

      while (chunk = io.read(DATA_READ_BYTES))
        if head.bytesize < STDERR_HEAD_BYTES
          take = STDERR_HEAD_BYTES - head.bytesize
          head << chunk.byteslice(0, take)
          chunk = chunk.byteslice(take..-1).to_s
        end
        next if chunk.empty?

        tail << chunk
        next unless tail.bytesize > STDERR_TAIL_BYTES

        excess = tail.bytesize - STDERR_TAIL_BYTES
        tail = tail.byteslice(excess..-1).to_s
        dropped += excess
      end

      return head + tail if dropped.zero?
      # Deliberate, and marked so nobody reads truncated output as a fault. See
      # STDERR_HEAD_BYTES/STDERR_TAIL_BYTES: everything was read, only the middle was dropped.
      "#{head}\n...[#{dropped} bytes omitted from the middle; all of it was read]...\n#{tail}"
    rescue IOError
      # Pipe closed underneath us during teardown; whatever was read is still useful.
      head + tail
    end

    ##
    # Joins a drain thread, giving up rather than hanging. See DRAIN_JOIN_TIMEOUT.
    def join_drain(thread)
      return thread.value if thread.join(DRAIN_JOIN_TIMEOUT)

      thread.kill
      Logger.warn Error.new(<<-EOS)
        A stream reader did not finish within #{DRAIN_JOIN_TIMEOUT}s of the pipeline
        exiting, so it was abandoned. Some output from this run was not captured.
        This is a deliberate trade -- see DRAIN_JOIN_TIMEOUT -- not a fault in itself:
        losing some STDERR is preferable to a backup that hangs with nothing in the log.
        The backup itself was unaffected; the pipeline had already exited.
      EOS
      nil
    end

    def record_stderr(collected)
      shell, *per_command = collected
      @stderr_messages = [["Pipeline", shell.to_s.strip]]
      per_command.each_with_index do |text, index|
        @stderr_messages << [command_name(@commands[index]), text.to_s.strip]
      end
    end

    ##
    # Logs each command's STDERR as its own warning, prefixed the way Utilities.run
    # prefixes it, so `ignore_warning` patterns match the same text they do today.
    def log_stderr
      @stderr_messages.each do |label, text|
        next if text.empty?
        Logger.warn(text.lines.map { |line| "#{label}:STDERR: #{line.chomp}\n" }.join)
      end
    end

    def check_statuses(report)
      reported = {}
      report.to_s.delete("\n").split(":").each do |status|
        index, exitstatus = status.split("|").map(&:to_i)
        reported[index] = exitstatus
      end

      @commands.each_with_index do |command, index|
        name = command_name(command)
        exitstatus = reported[index]

        if exitstatus.nil?
          # The `echo` never ran, so the subshell itself died -- killed by a signal, or the
          # shell could not start the command at all.
          @errors << Error.new("'#{name}' did not report an exit status (killed?)")
        elsif !@success_codes[index].include?(exitstatus)
          @errors << SystemCallError.new(
            "'#{name}' returned exit code: #{exitstatus}", exitstatus
          )
        end
      end
    end

    ##
    # Kills the whole process group, escalating to KILL if it does not go quietly.
    def terminate(pid)
      Process.kill("TERM", -pid)

      waited = 0.0
      until waited >= TERMINATE_GRACE
        return if Process.waitpid(pid, Process::WNOHANG)
        sleep 0.1
        waited += 0.1
      end

      Process.kill("KILL", -pid)
    rescue Errno::ESRCH, Errno::ECHILD
      # Already gone, which is the outcome this method wanted.
      nil
    end

    def reap(pid)
      Process.waitpid(pid)
    rescue Errno::ECHILD
      # Already reaped by #terminate.
      nil
    end

    # Not Linux, or above /proc/sys/fs/pipe-max-size, means the default capacity stands --
    # correct, just shallower -- so a failure here is reported and not raised.
    def widen_data_pipe(io)
      io.fcntl(F_SETPIPE_SZ, DATA_PIPE_BYTES)
      true
    rescue StandardError, NotImplementedError
      false
    end
  end
end
