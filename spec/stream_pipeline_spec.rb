require "spec_helper"
require "timeout"

# Every example here spawns a real child process.
#
# That is deliberate and not negotiable for this class. Pipe deadlocks do not exist in a
# world of StringIO doubles -- a double never fills, never blocks, and never refuses to exit
# -- so a spec built on doubles passes whether or not the streams are drained concurrently,
# which is precisely the bug it would be there to catch. The gem already learned this the
# expensive way: the pre-5.0.0.beta.5 Utilities.run deadlock was fully covered by stubbed
# specs that passed all along.
#
# Everything is wrapped in Timeout, because the failure mode under test is "hangs forever",
# and an rspec run that never finishes reports nothing.
describe "Backup::StreamPipeline" do
  let(:pipeline) { Backup::StreamPipeline.new }

  # Comfortably past the 64 KiB a pipe holds, so a writer that is not being drained will
  # block rather than buffer.
  let(:pipe_bytes) { 64 * 1024 }

  before do
    allow(Backup::Logger).to receive(:info)
    allow(Backup::Logger).to receive(:warn)
  end

  def run(&block)
    Timeout.timeout(60) { pipeline.run(&block) }
  end

  it "includes Utilities::Helpers" do
    expect(Backup::StreamPipeline.include?(Backup::Utilities::Helpers)).to eq(true)
  end

  describe "#run" do
    it "raises rather than spawning an empty pipeline" do
      expect { pipeline.run(&:read) }
        .to raise_error(Backup::StreamPipeline::Error, /No commands/)
    end

    it "yields the final command's STDOUT" do
      pipeline << %(ruby -e 'print "hello"')

      collected = nil
      run { |io| collected = io.read }

      expect(collected).to eq("hello")
    end

    it "pipes each stage's STDOUT into the next stage's STDIN" do
      pipeline << %(ruby -e 'print "hello"')
      pipeline << %(ruby -e 'print $stdin.read.upcase')
      pipeline << %(ruby -e 'print $stdin.read + "!"')

      collected = nil
      run { |io| collected = io.read }

      expect(collected).to eq("HELLO!")
    end

    it "returns true when every command succeeds" do
      pipeline << %(ruby -e 'print "x"')
      expect(run(&:read)).to be true
      expect(pipeline.success?).to be true
    end

    # The whole point of the class: the consumer gets the bytes as they are produced, so a
    # dump is never materialised anywhere.
    it "delivers data before the producer has finished" do
      pipeline << %(ruby -e '$stdout.write("first"); $stdout.flush; sleep 2; ) +
        %($stdout.write("second")')

      first = nil
      elapsed = nil
      started = Time.now
      run do |io|
        first = io.read(5)
        elapsed = Time.now - started
        io.read
      end

      expect(first).to eq("first")
      expect(elapsed).to be < 1.5
    end

    context "when a command writes more to STDERR than a pipe can hold" do
      # The shape that deadlocked Utilities.run: the child cannot exit while blocked writing
      # to a full STDERR pipe, so a parent that is not draining STDERR waits forever for an
      # EOF on STDOUT that can never come. A stream leans harder on this than a dump ever
      # did, because now there is a STDERR pipe per stage plus the exit-status pipe, and any
      # one of them left undrained is enough.
      let(:stderr_bytes) { pipe_bytes * 4 }

      it "completes, and keeps both the data and the STDERR" do
        pipeline << %(ruby -e '$stderr.write("e" * #{stderr_bytes}); $stderr.flush; ) +
          %(print "payload"')

        collected = nil
        run { |io| collected = io.read }

        expect(collected).to eq("payload")
        expect(pipeline.stderr_for(0).length).to eq(stderr_bytes)
      end

      it "completes when every stage does it at once, while data is flowing" do
        # Two stages, both talking over a full STDERR pipe, with megabytes of data moving
        # through at the same time. This is the streaming equivalent of mongodump and pigz
        # both reporting progress for an hour.
        payload_bytes = 4 * 1024 * 1024

        pipeline << %(ruby -e 'e = Thread.new { $stderr.write("a" * #{stderr_bytes}); ) +
          %($stderr.flush }; #{payload_bytes / 1024}.times { $stdout.write("o" * 1024) }; ) +
          %($stdout.flush; e.join')
        pipeline << %(ruby -e 'e = Thread.new { $stderr.write("b" * #{stderr_bytes}); ) +
          %($stderr.flush }; IO.copy_stream($stdin, $stdout); e.join')

        collected = 0
        run do |io|
          # Read in small bites, so the producers spend most of their time blocked on a full
          # data pipe. If STDERR were only drained after the data ran out, nothing would
          # ever finish.
          while (chunk = io.read(4096))
            collected += chunk.bytesize
          end
        end

        expect(collected).to eq(payload_bytes)
        # Each stage's STDERR is kept separately, even though both stages are "ruby".
        expect(pipeline.stderr_for(0).length).to eq(stderr_bytes)
        expect(pipeline.stderr_for(1).length).to eq(stderr_bytes)
      end
    end

    context "reporting STDERR" do
      # Utilities.run logs "<command>:STDERR: <line>", and config/backup.rb suppresses the
      # nightly mongodump chatter with an ignore_warning matched against that shape. The
      # streaming path has to log it identically or every backup starts reporting warnings.
      it "attributes each line to the command that wrote it" do
        pipeline << %(ruby -e '$stderr.puts "from ruby"; print "x"')
        pipeline << %(cat)

        warnings = []
        allow(Backup::Logger).to receive(:warn) { |msg| warnings << msg.to_s }

        run(&:read)

        expect(warnings.join).to include("ruby:STDERR: from ruby")
      end

      it "keeps the head and the tail when a command floods STDERR" do
        head = Backup::StreamPipeline::STDERR_HEAD_BYTES
        tail = Backup::StreamPipeline::STDERR_TAIL_BYTES
        total = (head + tail) * 3

        pipeline << %(ruby -e '$stderr.write("A" * #{head}); ) +
          %($stderr.write("M" * #{total - head - tail}); ) +
          %($stderr.write("Z" * #{tail}); $stderr.flush; print "x"')

        run(&:read)

        message = pipeline.stderr_for(0)
        expect(message).to start_with("A" * 100)
        expect(message).to end_with("Z" * 100)
        expect(message).to include("bytes omitted")
        # Bounded, rather than the 3x that was written.
        expect(message.length).to be < head + tail + 100
      end
    end

    context "when a command fails" do
      it "raises, naming the command and its exit status" do
        pipeline << %(ruby -e 'print "x"; exit 3')

        expect { run(&:read) }
          .to raise_error(Backup::StreamPipeline::Error, /'ruby' returned exit code: 3/)
        expect(pipeline.success?).to be false
      end

      it "detects a failure in the middle of the pipeline, not just the last command" do
        pipeline << %(ruby -e 'print "x"; exit 4')
        pipeline << %(cat)

        expect { run(&:read) }
          .to raise_error(Backup::StreamPipeline::Error, /exit code: 4/)
      end

      it "reports a command killed by a signal" do
        pipeline << %(ruby -e 'Process.kill("KILL", Process.pid)')

        expect { run(&:read) }
          .to raise_error(Backup::StreamPipeline::Error, /exit code: 137/)
      end

      it "accepts exit codes declared successful" do
        pipeline.add(%(ruby -e 'print "x"; exit 1'), [0, 1])

        expect(run(&:read)).to be true
      end

      it "includes the failing command's STDERR in the error" do
        pipeline << %(ruby -e '$stderr.puts("it went wrong") ; exit 1')

        expect { run(&:read) }
          .to raise_error(Backup::StreamPipeline::Error, /it went wrong/)
      end
    end

    context "when the consumer raises" do
      # A failed upload must not leave mongodump running against production for another
      # hour, blocked on a pipe with no reader.
      it "kills the pipeline and re-raises the consumer's exception unchanged" do
        pipeline << %(ruby -e 'loop { $stdout.write("x" * 1024) }')

        expect { run { |_io| raise ArgumentError, "upload blew up" } }
          .to raise_error(ArgumentError, "upload blew up")
      end

      it "does not report the dump as having failed when it was the consumer" do
        pipeline << %(ruby -e 'loop { $stdout.write("x" * 1024) }')

        # The pipeline's own exit status would be a signal death caused by our teardown.
        # Reporting that instead of the real error would send whoever reads the log
        # looking at mongodump.
        expect { run { |_io| raise "the real problem" } }
          .to raise_error(RuntimeError, "the real problem")
      end

      it "leaves no process behind" do
        # `sleep` ignores a closed STDOUT, so only killing the process group ends this.
        pipeline << %(ruby -e '$stdout.write("x"); $stdout.flush; sleep 120')

        children_before = `pgrep -f "sleep 120"`.split.size

        expect do
          run do |io|
            io.read(1)
            raise "stop"
          end
        end.to raise_error(RuntimeError)

        expect(`pgrep -f "sleep 120"`.split.size).to be <= children_before
      end
    end

    # A stream buffers nothing, so a consumer slower than the producer is the normal case
    # rather than an error: `common` produces ~39 MB/s of compressed output and a tenant that
    # compresses poorly produces far more, against an upload that may be slower still. What
    # must not happen is the parent quietly accumulating the difference in memory.
    context "when the consumer is slower than the producer" do
      # Asserted by watching the producer process rather than by measuring RSS. RSS is not
      # the claim and is not stable enough to be one -- it carries whatever the GC heap grew
      # to during the rest of the suite, so the same code measures differently depending on
      # what ran before it. Whether the producer is still blocked is exact.
      it "blocks the producer instead of absorbing the backlog" do
        marker = "sq3-1488-backpressure-#{Process.pid}"
        megabytes = 64

        # Writes 64 MiB as fast as it can and exits. Nothing here is slow on its own: if it
        # is still alive later, it is because the pipe is full and nobody is reading.
        pipeline << %(ruby -e '$PROGRAM_NAME = "#{marker}"; ) +
          %(#{megabytes}.times { $stdout.write("x" * 1024 * 1024) }')

        producer_alive_midway = nil
        received = 0

        run do |io|
          io.read(1024 * 1024) # take one chunk, then stop reading
          received += 1024 * 1024

          sleep 2 # far longer than writing 64 MiB to memory would take

          producer_alive_midway = system("pgrep -f #{marker} > /dev/null")

          while (chunk = io.read(1024 * 1024))
            received += chunk.bytesize
          end
        end

        # If anything between the producer and this block were buffering without bound, the
        # producer would have written all 64 MiB and exited during that sleep.
        expect(producer_alive_midway).to be true
        expect(received).to eq(megabytes * 1024 * 1024)
      end
    end

    it "does not hang when the consumer stops reading early" do
      pipeline.add(%(ruby -e 'loop { $stdout.write("x" * 4096) }'), [0, 1])

      expect do
        run { |io| io.read(4096) }
      end.to raise_error(Backup::StreamPipeline::Error)
    end
  end
end
