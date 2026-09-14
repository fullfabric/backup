require "backup/cloud_io/s3"

module Backup
  module Storage
    class S3 < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      ##
      # Amazon Simple Storage Service (S3) Credentials
      attr_accessor :access_key_id, :secret_access_key, :use_iam_profile

      ##
      # Amazon S3 bucket name
      attr_accessor :bucket

      ##
      # Region of the specified S3 bucket
      attr_accessor :region

      ##
      # Multipart chunk size, specified in MiB.
      #
      # Each package file larger than +chunk_size+
      # will be uploaded using S3 Multipart Upload.
      #
      # Minimum: 5 (but may be disabled with 0)
      # Maximum: 5120
      # Default: 5
      attr_accessor :chunk_size

      ##
      # Number of part uploads to run at once when storing from a stream.
      #
      # Only used by the streaming path. Uploading a finished file can afford to be serial,
      # because the file is not going anywhere. A stream cannot: nothing buffers it, so the
      # upload rate is a ceiling on the entire pipeline, and the serial loop's measured
      # 79 MB/s is below what a poorly-compressing tenant produces.
      #
      # Measured on p-db2 (SQ3-1482/SQ3-1487):
      #
      #   concurrency  1     79 MB/s
      #   concurrency  4    372 MB/s
      #   concurrency 10    819 MB/s
      #   concurrency 20    683 MB/s
      #
      # The default is 4 rather than the 10 at the knee, and that is a memory decision
      # rather than caution. Roughly 2 x concurrency x #chunk_size is in flight at once,
      # since each worker holds a part while the queue holds another: 512 MiB at 4 with a
      # 64 MiB #chunk_size, but 1.25 GiB at 10. This runs on the database host, where that
      # is page cache taken from Mongo for the length of the backup.
      #
      # 372 MB/s is not a compromise in practice. The largest tenant produces about
      # 39 MB/s of compressed output, and the worst-compressing one measured (klu, 2.1:1)
      # about 128 MB/s, so 4 already clears the requirement several times over and the
      # dump stays the bottleneck, which is the point. Raise it if that stops being true.
      #
      # Set to 1 to get the old serial behaviour without changing anything else.
      #
      # Default: 4
      attr_accessor :upload_concurrency

      ##
      # Number of times to retry failed operations.
      #
      # Default: 10
      attr_accessor :max_retries

      ##
      # Time in seconds to pause before each retry.
      #
      # Default: 30
      attr_accessor :retry_waitsec

      ##
      # Encryption algorithm to use for Amazon Server-Side Encryption
      #
      # Supported values:
      #
      # - :aes256
      #
      # Default: nil
      attr_accessor :encryption

      ##
      # Storage class to use for the S3 objects uploaded
      #
      # Supported values:
      #
      # - :standard (default)
      # - :standard_ia
      # - :reduced_redundancy
      #
      # Default: :standard
      attr_accessor :storage_class

      ##
      # Object tags to set on every uploaded object, as a URL-encoded query string.
      #
      # e.g. "tier=weekly" or "tier=weekly&source=mongo"
      #
      # Applied via the x-amz-tagging header, so the tag lands atomically with the object
      # rather than needing a PutObjectTagging call afterwards. This is what lets an S3
      # lifecycle rule filter on the tag, since lifecycle can only match on prefix, tag and
      # size, and cannot derive anything from the key or the upload time.
      #
      # Values are passed through verbatim; encode them if they contain characters that are
      # not safe in a query string.
      #
      # Default: nil
      attr_accessor :tagging

      ##
      # Additional options to pass along to fog.
      # e.g. Fog::Storage.new({ :provider => 'AWS' }.merge(fog_options))
      attr_accessor :fog_options

      def initialize(model, storage_id = nil)
        super

        @chunk_size         ||= 5 # MiB
        @upload_concurrency ||= 4
        @max_retries        ||= 10
        @retry_waitsec      ||= 30
        @path               ||= "backups"
        @storage_class      ||= :standard

        @path = @path.sub(/^\//, "")

        check_configuration
      end

      private

      def cloud_io
        @cloud_io ||= CloudIO::S3.new(
          access_key_id: access_key_id,
          secret_access_key: secret_access_key,
          use_iam_profile: use_iam_profile,
          region: region,
          bucket: bucket,
          encryption: encryption,
          storage_class: storage_class,
          tagging: tagging,
          max_retries: max_retries,
          retry_waitsec: retry_waitsec,
          chunk_size: chunk_size,
          upload_concurrency: upload_concurrency,
          fog_options: fog_options
        )
      end

      def transfer!
        package.filenames.each do |filename|
          src = File.join(Config.tmp_path, filename)
          dest = File.join(remote_path, filename)
          Logger.info "Storing '#{bucket}/#{dest}'..."
          cloud_io.upload(src, dest)
        end
      end

      ##
      # Uploads the live dump stream as a single multipart object, under the same key the
      # packaged path would have used.
      #
      # A stream is never split, so there is exactly one object here, as there is on the
      # packaged path for a model with no Splitter.
      def transfer_stream!(io)
        dest = File.join(remote_path, package.basename)
        Logger.info "Storing '#{bucket}/#{dest}' from a stream..."
        cloud_io.upload_stream(io, dest)
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{package.time}..."

        remote_path = remote_path_for(package)
        objects = cloud_io.objects(remote_path)

        raise Error, "Package at '#{remote_path}' not found" if objects.empty?

        cloud_io.delete(objects)
      end

      def check_configuration
        required =
          if use_iam_profile
            %w[bucket]
          else
            %w[access_key_id secret_access_key bucket]
          end
        raise Error, <<-EOS if required.map { |name| send(name) }.any?(&:nil?)
          Configuration Error
          #{required.map { |name| "##{name}" }.join(", ")} are all required
        EOS

        raise Error, <<-EOS if chunk_size > 0 && !chunk_size.between?(5, 5120)
          Configuration Error
          #chunk_size must be between 5 and 5120 (or 0 to disable multipart)
        EOS

        raise Error, <<-EOS unless upload_concurrency.to_i.between?(1, 32)
          Configuration Error
          #upload_concurrency must be between 1 and 32
        EOS

        raise Error, <<-EOS if encryption && encryption.to_s.upcase != "AES256"
          Configuration Error
          #encryption must be :aes256 or nil
        EOS

        classes = ["STANDARD", "STANDARD_IA", "REDUCED_REDUNDANCY"]
        raise Error, <<-EOS unless classes.include?(storage_class.to_s.upcase)
          Configuration Error
          #storage_class must be :standard or :standard_ia or :reduced_redundancy
        EOS
      end
    end
  end
end
