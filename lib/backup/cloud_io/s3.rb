require "backup/cloud_io/base"
require "fog"
require "digest/md5"
require "base64"
require "stringio"

module Backup
  module CloudIO
    class S3 < Base
      class Error < Backup::Error; end

      MAX_FILE_SIZE       = 1024**3 * 5   # 5 GiB
      MAX_MULTIPART_SIZE  = 1024**4 * 5   # 5 TiB

      # S3's hard limit on parts in one multipart upload.
      MAX_PARTS = 10_000

      # Minimum size S3 accepts for any part but the last.
      MIN_PART_SIZE = 1024**2 * 5

      # Part size used for a stream when #chunk_size is unusable (0, meaning "no multipart",
      # or below S3's minimum). A stream has no size to compare against a threshold, so it
      # is always multipart and always needs a workable part size.
      STREAM_PART_SIZE = 1024**2 * 64

      # A stream cannot know its length up front, so the part size cannot be chosen to fit
      # MAX_PARTS the way #adjusted_chunk_bytes does for a file. Instead the part size grows
      # as the stream runs. S3 only requires that every part but the last is at least
      # MIN_PART_SIZE -- parts need not be equal -- so this is legal, and it lifts the
      # ceiling from 640 GB (10,000 x 64 MiB) to 960 GB without holding more than 128 MiB
      # per part in memory.
      PART_SIZE_GROWTH_AFTER = 5_000
      MAX_STREAM_PART_SIZE   = 1024**2 * 128

      # Parts between progress lines. The total is unknown, so progress is reported as work
      # done rather than as a percentage.
      PARTS_PER_PROGRESS_LOG = 100

      ##
      # State shared between the reader and the part-upload workers.
      #
      # Every field is read and written under #mutex. #etags maps part number to ETag, and
      # #failure holds the first exception any worker hit -- which is both the signal that
      # tells the reader to stop and the exception that gets re-raised.
      PartUpload = Struct.new(:mutex, :etags, :failure)

      attr_reader :access_key_id, :secret_access_key, :use_iam_profile,
        :region, :bucket, :chunk_size, :upload_concurrency, :encryption,
        :storage_class, :tagging, :fog_options

      def initialize(options = {})
        super

        @access_key_id      = options[:access_key_id]
        @secret_access_key  = options[:secret_access_key]
        @use_iam_profile    = options[:use_iam_profile]
        @region             = options[:region]
        @bucket             = options[:bucket]
        @chunk_size         = options[:chunk_size]
        @upload_concurrency = options[:upload_concurrency]
        @encryption         = options[:encryption]
        @storage_class      = options[:storage_class]
        @tagging            = options[:tagging]
        @fog_options        = options[:fog_options]
      end

      # The Syncer may call this method in multiple threads.
      # However, #objects is always called prior to multithreading.
      def upload(src, dest)
        file_size = File.size(src)
        chunk_bytes = chunk_size * 1024**2
        if chunk_bytes > 0 && file_size > chunk_bytes
          raise FileSizeError, <<-EOS if file_size > MAX_MULTIPART_SIZE
            File Too Large
            File: #{src}
            Size: #{file_size}
            Max Multipart Upload Size is #{MAX_MULTIPART_SIZE} (5 TiB)
          EOS

          chunk_bytes = adjusted_chunk_bytes(chunk_bytes, file_size)
          upload_id = initiate_multipart(dest)
          parts = upload_parts(src, dest, upload_id, chunk_bytes, file_size)
          complete_multipart(dest, upload_id, parts)
        else
          raise FileSizeError, <<-EOS if file_size > MAX_FILE_SIZE
            File Too Large
            File: #{src}
            Size: #{file_size}
            Max File Size is #{MAX_FILE_SIZE} (5 GiB)
          EOS

          put_object(src, dest)
        end
      end

      # Uploads +io+ to +dest+ as a single object, reading it to EOF.
      #
      # Always a multipart upload: the length is not known in advance, so there is nothing
      # to compare against a threshold, and a single PUT would mean buffering the whole
      # object in memory to compute its Content-MD5.
      #
      # #headers is what carries x-amz-tagging, and S3 honours it on
      # InitiateMultipartUpload, so an object uploaded this way is tagged exactly as one
      # uploaded from a file is. The retention lifecycle rules filter on that tag.
      #
      # If anything fails, the multipart upload is aborted. An incomplete multipart upload
      # is never visible as an object, so a failed stream leaves nothing behind under the
      # prefix; aborting also stops it accruing storage charges for the uploaded parts.
      def upload_stream(io, dest)
        chunk_bytes = chunk_size.to_i * 1024**2
        if chunk_bytes < MIN_PART_SIZE
          Logger.info "\s\sStreaming uses #{STREAM_PART_SIZE / 1024**2} MiB parts " \
            "(#chunk_size is #{chunk_size.inspect}, which a stream cannot use)"
          chunk_bytes = STREAM_PART_SIZE
        end

        upload_id = initiate_multipart(dest)

        begin
          parts = upload_parts_from_stream(io, dest, upload_id, chunk_bytes)
          complete_multipart(dest, upload_id, parts)
        rescue Exception
          abort_multipart(dest, upload_id)
          raise
        end
      end

      # Returns all objects in the bucket with the given prefix.
      #
      # - #get_bucket returns a max of 1000 objects per request.
      # - Returns objects in alphabetical order.
      # - If marker is given, only objects after the marker are in the response.
      def objects(prefix)
        objects = []
        resp = nil
        prefix = prefix.chomp("/")
        opts = { "prefix" => prefix + "/" }

        while resp.nil? || resp.body["IsTruncated"]
          opts["marker"] = objects.last.key unless objects.empty?
          with_retries("GET '#{bucket}/#{prefix}/*'") do
            resp = connection.get_bucket(bucket, opts)
          end
          resp.body["Contents"].each do |obj_data|
            objects << Object.new(self, obj_data)
          end
        end

        objects
      end

      # Used by Object to fetch metadata if needed.
      def head_object(object)
        resp = nil
        with_retries("HEAD '#{bucket}/#{object.key}'") do
          resp = connection.head_object(bucket, object.key)
        end
        resp
      end

      # Delete object(s) from the bucket.
      #
      # - Called by the Storage (with objects) and the Syncer (with keys)
      # - Deletes 1000 objects per request.
      # - Missing objects will be ignored.
      def delete(objects_or_keys)
        keys = Array(objects_or_keys).dup
        keys.map!(&:key) if keys.first.is_a?(Object)

        opts = { quiet: true } # only report Errors in DeleteResult
        until keys.empty?
          keys_partial = keys.slice!(0, 1000)
          with_retries("DELETE Multiple Objects") do
            resp = connection.delete_multiple_objects(bucket, keys_partial, opts.dup)
            unless resp.body["DeleteResult"].empty?
              errors = resp.body["DeleteResult"].map do |result|
                error = result["Error"]
                "Failed to delete: #{error["Key"]}\n" \
                  "Reason: #{error["Code"]}: #{error["Message"]}"
              end.join("\n")
              raise Error, "The server returned the following:\n#{errors}"
            end
          end
        end
      end

      private

      def connection
        @connection ||= new_connection
      end

      # A fog connection wraps a single Excon connection, which is not safe to share across
      # threads: two requests interleaved on one socket corrupt each other. So every part
      # upload worker builds its own rather than reusing the memoized one.
      def new_connection
        opts = { provider: "AWS", region: region }
        if use_iam_profile
          opts[:use_iam_profile] = true
        else
          opts[:aws_access_key_id] = access_key_id
          opts[:aws_secret_access_key] = secret_access_key
        end
        opts.merge!(fog_options || {})
        conn = Fog::Storage.new(opts)
        conn.sync_clock
        conn
      end

      def put_object(src, dest)
        md5 = Base64.encode64(Digest::MD5.file(src).digest).chomp
        options = headers.merge("Content-MD5" => md5)
        with_retries("PUT '#{bucket}/#{dest}'") do
          File.open(src, "r") do |file|
            connection.put_object(bucket, dest, file, options)
          end
        end
      end

      def initiate_multipart(dest)
        Logger.info "\s\sInitiate Multipart '#{bucket}/#{dest}'"

        # Creation time is the only time this can be got right.
        #
        # The cross-account vault replication rule (SQ3-1440) filters on tier=weekly and is
        # evaluated when the object is created; it never re-fires if the tag is changed
        # afterwards, so lib/backups/tagger.rb cannot repair a miss. Past seven days that
        # vault is the only copy of the database outside eu-west-1, which makes an untagged
        # Monday a permanent hole in it with nothing alarming on it -- the SQ3-1113 failure
        # mode of a silent absence.
        #
        # So if tagging was asked for and is not on its way in this request, stop here,
        # before a multi-hour dump produces an object that can never be replicated.
        if !tagging.to_s.empty? && headers["x-amz-tagging"].to_s.empty?
          raise Error, <<-EOS
            Object Tagging Lost
            #tagging is set to #{tagging.inspect}, but no x-amz-tagging header was built
            for '#{bucket}/#{dest}'. Tags are only applied at object creation, and both the
            retention lifecycle rules and the cross-account vault replication filter on
            them, so this upload has been stopped rather than written untagged.
          EOS
        end

        resp = nil
        with_retries("POST '#{bucket}/#{dest}' (Initiate)") do
          resp = connection.initiate_multipart_upload(bucket, dest, headers)
        end
        resp.body["UploadId"]
      end

      # Each part's MD5 is sent to verify the transfer.
      # AWS will concatenate all parts into a single object
      # once the multipart upload is completed.
      def upload_parts(src, dest, upload_id, chunk_bytes, file_size)
        total_parts = (file_size / chunk_bytes.to_f).ceil
        progress = (0.1..0.9).step(0.1).map { |n| (total_parts * n).floor }
        Logger.info "\s\sUploading #{total_parts} Parts..."

        parts = []
        File.open(src, "r") do |file|
          part_number = 0
          while data = file.read(chunk_bytes)
            part_number += 1
            md5 = Base64.encode64(Digest::MD5.digest(data)).chomp

            with_retries("PUT '#{bucket}/#{dest}' Part ##{part_number}") do
              resp = connection.upload_part(
                bucket, dest, upload_id, part_number, StringIO.new(data),
                "Content-MD5" => md5
              )
              parts << resp.headers["ETag"]
            end

            if i = progress.rindex(part_number)
              Logger.info "\s\s...#{i + 1}0% Complete..."
            end
          end
        end
        parts
      end

      # Reads +io+ to EOF, uploading each chunk as a part.
      #
      # The read loop itself is the same shape as #upload_parts: IO#read(n) blocks until it
      # has n bytes or hits EOF, on a pipe exactly as on a file. What differs is that the
      # length is unknown, so the part count cannot be planned, and that parts are uploaded
      # by a pool of workers rather than one at a time.
      #
      # == Backpressure
      #
      # Nothing buffers the dump. This method reading slower than the dump produces is what
      # pushes back through the pipeline: the queue fills, the reader stops reading, the
      # pipe fills, and the compressor and then mongodump block. That is correct behaviour
      # and not an error -- but it does mean the upload rate is a ceiling on the whole run,
      # which is why #upload_concurrency exists.
      #
      # == Failure
      #
      # A worker that exhausts its retries records the error and keeps popping the queue
      # without uploading. It must keep popping: if workers exited on error, the reader
      # would block forever on a queue nobody drains. The reader checks for a recorded
      # error before each read and gives up, which closes the pipe and takes the dump down
      # with it rather than reading another 90 GB into a dead upload.
      #
      # Retries are safe because with_retries re-sends +data+, which is already in memory.
      def upload_parts_from_stream(io, dest, upload_id, chunk_bytes)
        concurrency = [upload_concurrency.to_i, 1].max
        Logger.info "\s\sUploading from a stream in #{chunk_bytes / 1024**2} MiB parts, " \
          "#{concurrency} at a time..."

        state = PartUpload.new(Mutex.new, {}, error: nil)
        queue = SizedQueue.new(concurrency)

        workers = start_part_workers(concurrency, queue, dest, upload_id, state)

        part_number = 0
        total_bytes = 0

        begin
          loop do
            if (err = state.mutex.synchronize { state.failure[:error] })
              raise err
            end

            data = io.read(chunk_bytes)
            break if data.nil? || data.empty?

            part_number += 1
            total_bytes += data.bytesize

            raise Error, <<-EOS if part_number > MAX_PARTS
              Stream Too Large
              Object: #{bucket}/#{dest}
              This stream needs more than the #{MAX_PARTS} parts S3 allows in one upload,
              even after growing the part size to #{MAX_STREAM_PART_SIZE / 1024**2} MiB.
              Raise #chunk_size on the Storage to take larger parts.
            EOS

            queue.push([part_number, data])
            chunk_bytes = grown_part_size(chunk_bytes, part_number)
            log_stream_progress(part_number, total_bytes)
          end
        ensure
          # One nil per worker, and workers exit on nil, so each consumes exactly one. The
          # queue may be full, in which case these block until the workers drain it -- which
          # they will, error or not.
          concurrency.times { queue.push(nil) }
          workers.each(&:join)
        end

        if (err = state.mutex.synchronize { state.failure[:error] })
          raise err
        end

        # Independent of the error flag on purpose: a part that went missing without anyone
        # raising must not produce a completed object either.
        raise Error, <<-EOS unless state.etags.size == part_number
          Incomplete Multipart Upload
          Object: #{bucket}/#{dest}
          Read #{part_number} parts but only #{state.etags.size} were acknowledged.
        EOS

        Logger.info "\s\sUploaded #{part_number} Parts, " \
          "#{total_bytes / 1024**2} MiB total."

        # complete_multipart needs the ETags in part order, which is not the order they were
        # acknowledged in.
        state.etags.sort.map(&:last)
      end

      # Starts the pool that uploads parts off +queue+.
      #
      # A worker records a failure through +on_error+ and then keeps popping without
      # uploading. It must keep popping: if workers exited on error, the reader would block
      # forever pushing to a queue nobody drains. The reader is what notices and stops.
      #
      # ETags are placed into +etags+ BY PART NUMBER, never appended. Parts finish in
      # whatever order the network gives them, and CompleteMultipartUpload matched to the
      # wrong part numbers assembles an object that uploads cleanly and is garbage at
      # restore time.
      def start_part_workers(concurrency, queue, dest, upload_id, state)
        Array.new(concurrency) do
          Thread.new do
            connection = new_connection

            while (job = queue.pop)
              part_number, data = job

              begin
                next if state.mutex.synchronize { state.failure[:error] }

                md5 = Base64.encode64(Digest::MD5.digest(data)).chomp
                with_retries("PUT '#{bucket}/#{dest}' Part ##{part_number}") do
                  resp = connection.upload_part(
                    bucket, dest, upload_id, part_number, StringIO.new(data),
                    "Content-MD5" => md5
                  )
                  state.mutex.synchronize do
                    state.etags[part_number] = resp.headers["ETag"]
                  end
                end
              rescue Exception => err
                state.mutex.synchronize { state.failure[:error] ||= err }
              end
            end
          end
        end
      end

      # Doubles the part size once the stream passes PART_SIZE_GROWTH_AFTER parts, so an
      # object larger than MAX_PARTS x the starting part size still fits. Only affects reads
      # after this point; the parts already sent keep their size, which S3 allows.
      def grown_part_size(chunk_bytes, part_number)
        return chunk_bytes unless (part_number % PART_SIZE_GROWTH_AFTER).zero?
        return chunk_bytes if chunk_bytes >= MAX_STREAM_PART_SIZE

        grown = [chunk_bytes * 2, MAX_STREAM_PART_SIZE].min
        Logger.warn Error.new(<<-EOS)
          Stream Part Size Increased
          After #{part_number} parts this stream is still going, so the part size has been
          raised from #{chunk_bytes / 1024**2} MiB to #{grown / 1024**2} MiB to stay within
          the #{MAX_PARTS} part limit. Consider raising #chunk_size on the Storage.
        EOS
        grown
      end

      def log_stream_progress(part_number, total_bytes)
        return unless (part_number % PARTS_PER_PROGRESS_LOG).zero?
        Logger.info "\s\s...#{part_number} Parts, #{total_bytes / 1024**2} MiB uploaded..."
      end

      # Discards a multipart upload and the parts already sent.
      #
      # Best-effort and deliberately not retried: this runs on a path that is already
      # failing, and spending max_retries x retry_waitsec here would delay reporting the
      # real error by minutes. A bucket lifecycle rule with AbortIncompleteMultipartUpload
      # is the backstop if this does not land.
      def abort_multipart(dest, upload_id)
        Logger.info "\s\sAbort Multipart '#{bucket}/#{dest}'"
        connection.abort_multipart_upload(bucket, dest, upload_id)
      rescue Exception => err
        Logger.warn Error.wrap(err, <<-EOS)
          Failed to abort the multipart upload for '#{bucket}/#{dest}'.
          No object was created, but the parts already uploaded will be billed until the
          bucket's AbortIncompleteMultipartUpload lifecycle rule removes them.
          Upload ID: #{upload_id}
        EOS
      end

      def complete_multipart(dest, upload_id, parts)
        Logger.info "\s\sComplete Multipart '#{bucket}/#{dest}'"

        with_retries("POST '#{bucket}/#{dest}' (Complete)") do
          resp = connection.complete_multipart_upload(bucket, dest, upload_id, parts)
          raise Error, <<-EOS if resp.body["Code"]
            The server returned the following error:
            #{resp.body["Code"]}: #{resp.body["Message"]}
          EOS
        end
      end

      def headers
        headers = {}

        enc = encryption.to_s.upcase
        headers["x-amz-server-side-encryption"] = enc unless enc.empty?

        sc = storage_class.to_s.upcase
        headers["x-amz-storage-class"] = sc unless sc.empty? || sc == "STANDARD"

        # Object tags, as a URL-encoded query string, e.g. "tier=weekly". Set here rather
        # than applied afterwards so the tag lands atomically with the object and there is
        # no window in which it exists untagged. This method feeds both put_object and
        # initiate_multipart_upload, and S3 honours x-amz-tagging on both, so single-part
        # and multipart uploads are tagged alike.
        tags = tagging.to_s
        headers["x-amz-tagging"] = tags unless tags.empty?

        headers
      end

      def adjusted_chunk_bytes(chunk_bytes, file_size)
        return chunk_bytes if file_size / chunk_bytes.to_f <= 10_000

        mb = orig_mb = chunk_bytes / 1024**2
        mb += 1 until file_size / (1024**2 * mb).to_f <= 10_000
        Logger.warn Error.new(<<-EOS)
          Chunk Size Adjusted
          Your original #chunk_size of #{orig_mb} MiB has been adjusted
          to #{mb} MiB in order to satisfy the limit of 10,000 chunks.
          To enforce your chosen #chunk_size, you should use the Splitter.
          e.g. split_into_chunks_of #{mb * 10_000} (#chunk_size * 10_000)
        EOS
        1024**2 * mb
      end

      class Object
        attr_reader :key, :etag, :storage_class

        def initialize(cloud_io, data)
          @cloud_io = cloud_io
          @key  = data["Key"]
          @etag = data["ETag"]
          @storage_class = data["StorageClass"]
        end

        # currently 'AES256' or nil
        def encryption
          metadata["x-amz-server-side-encryption"]
        end

        private

        def metadata
          @metadata ||= @cloud_io.head_object(self).headers
        end
      end
    end
  end
end
