module Backup
  module Database
    class MongoDB < Base
      class Error < Backup::Error; end

      ##
      # Name of the database that needs to get dumped
      attr_accessor :name

      ##
      # Credentials for the specified database
      attr_accessor :username, :password, :authdb

      ##
      # Connectivity options
      attr_accessor :host, :port

      ##
      # IPv6 support (disabled by default)
      attr_accessor :ipv6

      ##
      # Collections to dump, collections that aren't specified won't get dumped
      attr_accessor :only_collections

      ##
      # Additional "mongodump" options
      attr_accessor :additional_options

      ##
      # Forces mongod to flush all pending write operations to the disk and
      # locks the entire mongod instance to prevent additional writes until the
      # dump is complete.
      #
      # Note that if Profiling is enabled, this will disable it and will not
      # re-enable it after the dump is complete.
      attr_accessor :lock

      ##
      # Creates a dump of the database that includes an oplog, to create a
      # point-in-time snapshot of the state of a mongod instance.
      #
      # If this option is used, you would not use the `lock` option.
      #
      # This will only work against nodes that maintain a oplog.
      # This includes all members of a replica set, as well as master nodes in
      # master/slave replication deployments.
      attr_accessor :oplog

      def initialize(model, database_id = nil, &block)
        super
        instance_eval(&block) if block_given?
      end

      def perform!
        super

        lock_database if @lock
        dump!
        package!
      ensure
        unlock_database if @lock
      end

      ##
      # Dumps straight to a pipe with `mongodump --archive`, compressing on the way, and
      # yields the IO carrying the compressed archive along with the extension it should be
      # stored under. Nothing is written to disk.
      #
      # `--archive` with no argument writes the whole dump to STDOUT as a single stream,
      # which is what makes this possible at all; it is restored with `mongorestore
      # --archive`, not by pointing mongorestore at a dump directory.
      #
      # The yielded IO must be read to EOF by the block. See StreamPipeline for what
      # happens if it is not.
      def dump_stream
        raise Error, <<-EOS if stream_unsupported_reason
          Cannot stream this database
          #{stream_unsupported_reason}
        EOS

        log!(:started)
        lock_database if @lock

        pipeline = StreamPipeline.new
        extension = +"archive"

        pipeline << mongodump_archive

        if model.compressor
          model.compressor.compress_with do |command, ext|
            pipeline << command
            extension << ext
          end
        end

        pipeline.run { |io| yield io, extension }
        log!(:finished)
      ensure
        unlock_database if @lock
      end

      ##
      # Whether this database can be dumped as a single stream.
      def streamable?
        stream_unsupported_reason.nil?
      end

      ##
      # Why it cannot be, as a sentence for the log, or nil if it can.
      def stream_unsupported_reason
        return nil if Array(only_collections).empty?

        "#only_collections runs one mongodump per collection, and concatenating several " \
          "--archive streams does not produce a restorable archive. Use " \
          "--excludeCollection in #additional_options instead."
      end

      private

      ##
      # Performs all required mongodump commands, dumping the output files
      # into the +dump_packaging_path+ directory for packaging.
      def dump!
        FileUtils.mkdir_p dump_packaging_path

        collections = Array(only_collections)
        if collections.empty?
          run(mongodump)
        else
          collections.each do |collection|
            run("#{mongodump} --collection='#{collection}'")
          end
        end
      end

      ##
      # Creates a tar archive of the +dump_packaging_path+ directory
      # and stores it in the +dump_path+ using +dump_filename+.
      #
      #   <trigger>/databases/MongoDB[-<database_id>].tar[.gz]
      #
      # If successful, +dump_packaging_path+ is removed.
      def package!
        pipeline = Pipeline.new
        dump_ext = "tar"

        pipeline << "#{utility(:tar)} -cf - " \
            "-C '#{dump_path}' '#{dump_filename}'"

        if model.compressor
          model.compressor.compress_with do |command, ext|
            pipeline << command
            dump_ext << ext
          end
        end

        pipeline << "#{utility(:cat)} > " \
          "'#{File.join(dump_path, dump_filename)}.#{dump_ext}'"

        pipeline.run
        if pipeline.success?
          FileUtils.rm_rf dump_packaging_path
          log!(:finished)
        else
          raise Error, "Dump Failed!\n#{pipeline.error_messages}"
        end
      end

      def dump_packaging_path
        File.join(dump_path, dump_filename)
      end

      ##
      # Options common to both output modes. Kept separate from the `--out` and `--archive`
      # flags so that neither destination can leak into the other's command line: a change
      # verified against `--archive` and then shipped as `--out` is exactly the mistake this
      # split exists to make impossible.
      def mongodump_options
        "#{utility(:mongodump)} #{name_option} #{credential_options} " \
          "#{connectivity_options} #{ipv6_option} #{oplog_option} " \
          "#{user_options}"
      end

      # Writes a dump directory, for the package-on-disk path.
      def mongodump
        "#{mongodump_options} --out='#{dump_packaging_path}'"
      end

      # Writes a single archive to STDOUT, for the streaming path.
      def mongodump_archive
        "#{mongodump_options} --archive"
      end

      def name_option
        return unless name
        "--db='#{name}'"
      end

      def credential_options
        opts = []
        opts << "--username='#{username}'" if username
        opts << "--password='#{password}'" if password
        opts << "--authenticationDatabase='#{authdb}'" if authdb
        opts.join(" ")
      end

      def connectivity_options
        opts = []
        opts << "--host='#{host}'" if host
        opts << "--port='#{port}'" if port
        opts.join(" ")
      end

      def ipv6_option
        "--ipv6" if ipv6
      end

      def oplog_option
        "--oplog" if oplog
      end

      def user_options
        Array(additional_options).join(" ")
      end

      def lock_database
        lock_command = <<-EOS.gsub(/^ +/, "")
          echo 'use admin
          db.setProfilingLevel(0)
          db.fsyncLock()' | #{mongo_shell}
        EOS

        run(lock_command)
      end

      def unlock_database
        unlock_command = <<-EOS.gsub(/^ +/, "")
          echo 'use admin
          db.fsyncUnlock()' | #{mongo_shell}
        EOS

        run(unlock_command)
      end

      def mongo_shell
        cmd = "#{utility(:mongo)} #{connectivity_options}".rstrip
        cmd << " #{credential_options}".rstrip
        cmd << " #{ipv6_option}".rstrip
        cmd << " '#{name}'" if name
        cmd
      end
    end
  end
end
