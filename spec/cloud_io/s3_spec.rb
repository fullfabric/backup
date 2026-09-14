require "spec_helper"
require "backup/cloud_io/s3"
require "timeout"

module Backup # rubocop:disable Metrics/ModuleLength
  describe CloudIO::S3 do # rubocop:disable Metrics/BlockLength
    let(:connection) { double }

    describe "#upload" do
      context "with multipart support" do
        let(:cloud_io) { CloudIO::S3.new(bucket: "my_bucket", chunk_size: 5) }
        let(:parts) { double }

        context "when src file is larger than chunk_size" do
          before do
            expect(File).to receive(:size).with("/src/file").and_return(10 * 1024**2)
          end

          it "uploads using multipart" do
            expect(cloud_io).to receive(:initiate_multipart).with("dest/file").and_return(1234)
            expect(cloud_io).to receive(:upload_parts).with(
              "/src/file", "dest/file", 1234, 5 * 1024**2, 10 * 1024**2
            ).and_return(parts)
            expect(cloud_io).to receive(:complete_multipart).with("dest/file", 1234, parts)
            expect(cloud_io).to receive(:put_object).never

            cloud_io.upload("/src/file", "dest/file")
          end
        end

        context "when src file is not larger than chunk_size" do
          before do
            expect(File).to receive(:size).with("/src/file").and_return(5 * 1024**2)
          end

          it "uploads without multipart" do
            expect(cloud_io).to receive(:put_object).with("/src/file", "dest/file")
            expect(cloud_io).to receive(:initiate_multipart).never

            cloud_io.upload("/src/file", "dest/file")
          end
        end

        context "when chunk_size is too small for the src file" do
          before do
            expect(File).to receive(:size).with("/src/file").and_return((50_000 * 1024**2) + 1)
          end

          it "warns and adjusts the chunk_size" do
            expect(cloud_io).to receive(:initiate_multipart).with("dest/file").and_return(1234)
            expect(cloud_io).to receive(:upload_parts).with(
              "/src/file", "dest/file", 1234, 6 * 1024**2, (50_000 * 1024**2) + 1
            ).and_return(parts)
            expect(cloud_io).to receive(:complete_multipart).with("dest/file", 1234, parts)
            expect(cloud_io).to receive(:put_object).never

            expect(Logger).to receive(:warn) do |err|
              expect(err.message).to include(
                "#chunk_size of 5 MiB has been adjusted\n  to 6 MiB"
              )
            end

            cloud_io.upload("/src/file", "dest/file")
          end
        end

        context "when src file is too large" do
          before do
            expect(File).to receive(:size).with("/src/file")
              .and_return(described_class::MAX_MULTIPART_SIZE + 1)
          end

          it "raises an error" do
            expect(cloud_io).to receive(:initiate_multipart).never
            expect(cloud_io).to receive(:put_object).never

            expect do
              cloud_io.upload("/src/file", "dest/file")
            end.to raise_error(CloudIO::FileSizeError)
          end
        end
      end # context 'with multipart support'

      context "without multipart support" do
        let(:cloud_io) { CloudIO::S3.new(bucket: "my_bucket", chunk_size: 0) }

        before do
          expect(cloud_io).to receive(:initiate_multipart).never
        end

        context "when src file size is ok" do
          before do
            expect(File).to receive(:size).with("/src/file")
              .and_return(described_class::MAX_FILE_SIZE)
          end

          it "uploads using put_object" do
            expect(cloud_io).to receive(:put_object).with("/src/file", "dest/file")

            cloud_io.upload("/src/file", "dest/file")
          end
        end

        context "when src file is too large" do
          before do
            expect(File).to receive(:size).with("/src/file")
              .and_return(described_class::MAX_FILE_SIZE + 1)
          end

          it "raises an error" do
            expect(cloud_io).to receive(:put_object).never

            expect do
              cloud_io.upload("/src/file", "dest/file")
            end.to raise_error(CloudIO::FileSizeError)
          end
        end
      end # context 'without multipart support'
    end # describe '#upload'

    describe "#objects" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
      end

      it "ensures prefix ends with /" do
        expect(connection).to receive(:get_bucket)
          .with("my_bucket", "prefix" => "foo/bar/")
          .and_return(double("response", body: { "Contents" => [] }))
        expect(cloud_io.objects("foo/bar")).to eq []
      end

      it "returns an empty array when no objects are found" do
        expect(connection).to receive(:get_bucket)
          .with("my_bucket", "prefix" => "foo/bar/")
          .and_return(double("response", body: { "Contents" => [] }))
        expect(cloud_io.objects("foo/bar/")).to eq []
      end

      context "when returned objects are not truncated" do
        let(:resp_body) do
          { "IsTruncated" => false,
            "Contents" => Array.new(10) do |n|
              { "Key" => "key_#{n}",
                "ETag" => "etag_#{n}",
                "StorageClass" => "STANDARD" }
            end }
        end

        it "returns all objects" do
          expect(cloud_io).to receive(:with_retries)
            .with("GET 'my_bucket/foo/bar/*'").and_yield
          expect(connection).to receive(:get_bucket)
            .with("my_bucket", "prefix" => "foo/bar/")
            .and_return(double("response", body: resp_body))

          objects = cloud_io.objects("foo/bar/")
          expect(objects.count).to be 10
          objects.each_with_index do |object, n|
            expect(object.key).to eq("key_#{n}")
            expect(object.etag).to eq("etag_#{n}")
            expect(object.storage_class).to eq("STANDARD")
          end
        end
      end

      context "when returned objects are truncated" do
        let(:resp_body_a) do
          { "IsTruncated" => true,
            "Contents" => (0..6).map do |n|
              { "Key" => "key_#{n}",
                "ETag" => "etag_#{n}",
                "StorageClass" => "STANDARD" }
            end }
        end
        let(:resp_body_b) do
          { "IsTruncated" => false,
            "Contents" => (7..9).map do |n|
              { "Key" => "key_#{n}",
                "ETag" => "etag_#{n}",
                "StorageClass" => "STANDARD" }
            end }
        end

        it "returns all objects" do
          expect(cloud_io).to receive(:with_retries).twice
            .with("GET 'my_bucket/foo/bar/*'").and_yield
          expect(connection).to receive(:get_bucket)
            .with("my_bucket", "prefix" => "foo/bar/")
            .and_return(double("response", body: resp_body_a))
          expect(connection).to receive(:get_bucket)
            .with("my_bucket", "prefix" => "foo/bar/", "marker" => "key_6")
            .and_return(double("response", body: resp_body_b))

          objects = cloud_io.objects("foo/bar/")
          expect(objects.count).to be 10
          objects.each_with_index do |object, n|
            expect(object.key).to eq("key_#{n}")
            expect(object.etag).to eq("etag_#{n}")
            expect(object.storage_class).to eq("STANDARD")
          end
        end

        it "retries on errors" do
          expect(connection).to receive(:get_bucket).once
            .with("my_bucket", "prefix" => "foo/bar/")
            .and_raise("error")
          expect(connection).to receive(:get_bucket).once
            .with("my_bucket", "prefix" => "foo/bar/")
            .and_return(double("response", body: resp_body_a))
          expect(connection).to receive(:get_bucket).once
            .with("my_bucket", "prefix" => "foo/bar/", "marker" => "key_6")
            .and_raise("error")
          expect(connection).to receive(:get_bucket).once
            .with("my_bucket", "prefix" => "foo/bar/", "marker" => "key_6")
            .and_return(double("response", body: resp_body_b))

          objects = cloud_io.objects("foo/bar/")
          expect(objects.count).to be 10
          objects.each_with_index do |object, n|
            expect(object.key).to eq("key_#{n}")
            expect(object.etag).to eq("etag_#{n}")
            expect(object.storage_class).to eq("STANDARD")
          end
        end
      end
    end # describe '#objects'

    describe "#head_object" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
      end

      it "returns head_object response with retries" do
        object = double("response", key: "obj_key")
        expect(connection).to receive(:head_object).once
          .with("my_bucket", "obj_key")
          .and_raise("error")
        expect(connection).to receive(:head_object).once
          .with("my_bucket", "obj_key")
          .and_return(:response)
        expect(cloud_io.head_object(object)).to eq :response
      end
    end # describe '#head_object'

    describe "#delete" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:resp_ok) { double("response", body: { "DeleteResult" => [] }) }
      let(:resp_bad) do
        double(
          "response",
          body: {
            "DeleteResult" => [
              { "Error" => {
                "Key" => "obj_key",
                "Code" => "InternalError",
                "Message" => "We encountered an internal error. Please try again."
              } }
            ]
          }
        )
      end

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
      end

      it "accepts a single Object" do
        object = described_class::Object.new(:foo, "Key" => "obj_key")
        expect(cloud_io).to receive(:with_retries).with("DELETE Multiple Objects").and_yield
        expect(connection).to receive(:delete_multiple_objects).with(
          "my_bucket", ["obj_key"], quiet: true
        ).and_return(resp_ok)
        cloud_io.delete(object)
      end

      it "accepts multiple Objects" do
        object_a = described_class::Object.new(:foo, "Key" => "obj_key_a")
        object_b = described_class::Object.new(:foo, "Key" => "obj_key_b")
        expect(cloud_io).to receive(:with_retries).with("DELETE Multiple Objects").and_yield
        expect(connection).to receive(:delete_multiple_objects).with(
          "my_bucket", ["obj_key_a", "obj_key_b"], quiet: true
        ).and_return(resp_ok)

        objects = [object_a, object_b]
        expect { cloud_io.delete(objects) }.not_to change { objects }
      end

      it "accepts a single key" do
        expect(cloud_io).to receive(:with_retries).with("DELETE Multiple Objects").and_yield
        expect(connection).to receive(:delete_multiple_objects).with(
          "my_bucket", ["obj_key"], quiet: true
        ).and_return(resp_ok)
        cloud_io.delete("obj_key")
      end

      it "accepts multiple keys" do
        expect(cloud_io).to receive(:with_retries).with("DELETE Multiple Objects").and_yield
        expect(connection).to receive(:delete_multiple_objects).with(
          "my_bucket", ["obj_key_a", "obj_key_b"], quiet: true
        ).and_return(resp_ok)

        objects = ["obj_key_a", "obj_key_b"]
        expect { cloud_io.delete(objects) }.not_to change { objects }
      end

      it "does nothing if empty array passed" do
        expect(connection).to receive(:delete_multiple_objects).never
        cloud_io.delete([])
      end

      context "with more than 1000 objects" do
        let(:keys_1k) { Array.new(1000) { "key" } }
        let(:keys_10) { Array.new(10) { "key" } }
        let(:keys_all) { keys_1k + keys_10 }

        before do
          expect(cloud_io).to receive(:with_retries).twice.with("DELETE Multiple Objects").and_yield
        end

        it "deletes 1000 objects per request" do
          expect(connection).to receive(:delete_multiple_objects).with(
            "my_bucket", keys_1k, quiet: true
          ).and_return(resp_ok)
          expect(connection).to receive(:delete_multiple_objects).with(
            "my_bucket", keys_10, quiet: true
          ).and_return(resp_ok)

          expect { cloud_io.delete(keys_all) }.not_to change { keys_all }
        end

        it "prevents mutation of options to delete_multiple_objects" do
          expect(connection).to receive(:delete_multiple_objects) do |bucket, keys, opts|
            bucket == "my_bucket" && keys == keys_1k && opts.delete(:quiet)
          end.and_return(resp_ok)
          expect(connection).to receive(:delete_multiple_objects).with(
            "my_bucket", keys_10, quiet: true
          ).and_return(resp_ok)

          expect { cloud_io.delete(keys_all) }.not_to change { keys_all }
        end
      end

      it "retries on raised errors" do
        expect(connection).to receive(:delete_multiple_objects).once
          .with("my_bucket", ["obj_key"], quiet: true)
          .and_raise("error")
        expect(connection).to receive(:delete_multiple_objects).once
          .with("my_bucket", ["obj_key"], quiet: true)
          .and_return(resp_ok)
        cloud_io.delete("obj_key")
      end

      it "retries on returned errors" do
        expect(connection).to receive(:delete_multiple_objects).twice
          .with("my_bucket", ["obj_key"], quiet: true)
          .and_return(resp_bad, resp_ok)
        cloud_io.delete("obj_key")
      end

      it "fails after retries exceeded" do
        expect(connection).to receive(:delete_multiple_objects).once
          .with("my_bucket", ["obj_key"], quiet: true)
          .and_raise("error message")
        expect(connection).to receive(:delete_multiple_objects).once
          .with("my_bucket", ["obj_key"], quiet: true)
          .and_return(resp_bad)

        expect do
          cloud_io.delete("obj_key")
        end.to raise_error CloudIO::Error, "CloudIO::Error: Max Retries (1) Exceeded!\n" \
          "  Operation: DELETE Multiple Objects\n" \
          "  Be sure to check the log messages for each retry attempt.\n" \
          "--- Wrapped Exception ---\n" \
          "CloudIO::S3::Error: The server returned the following:\n" \
          "  Failed to delete: obj_key\n" \
          "  Reason: InternalError: We encountered an internal error. " \
            "Please try again."
        expect(Logger.messages.map(&:lines).join("\n")).to eq(
          "CloudIO::Error: Retry #1 of 1\n" \
          "  Operation: DELETE Multiple Objects\n" \
          "--- Wrapped Exception ---\n" \
          "RuntimeError: error message"
        )
      end
    end # describe '#delete'

    describe "#connection" do
      specify "using AWS access keys" do
        expect(Fog::Storage).to receive(:new).once.with(
          provider: "AWS",
          aws_access_key_id: "my_access_key_id",
          aws_secret_access_key: "my_secret_access_key",
          region: "my_region"
        ).and_return(connection)
        expect(connection).to receive(:sync_clock).once

        cloud_io = CloudIO::S3.new(
          access_key_id: "my_access_key_id",
          secret_access_key: "my_secret_access_key",
          region: "my_region"
        )

        expect(cloud_io.send(:connection)).to be connection
        expect(cloud_io.send(:connection)).to be connection
      end

      specify "using AWS IAM profile" do
        expect(Fog::Storage).to receive(:new).once.with(
          provider: "AWS",
          use_iam_profile: true,
          region: "my_region"
        ).and_return(connection)
        expect(connection).to receive(:sync_clock).once

        cloud_io = CloudIO::S3.new(
          use_iam_profile: true,
          region: "my_region"
        )

        expect(cloud_io.send(:connection)).to be connection
        expect(cloud_io.send(:connection)).to be connection
      end

      it "passes along fog_options" do
        expect(Fog::Storage).to receive(:new).with(provider: "AWS",
                                                   region: nil,
                                                   aws_access_key_id: "my_key",
                                                   aws_secret_access_key: "my_secret",
                                                   connection_options: { opt_key: "opt_value" },
                                                   my_key: "my_value").and_return(double("response", sync_clock: nil))
        CloudIO::S3.new(
          access_key_id: "my_key",
          secret_access_key: "my_secret",
          fog_options: {
            connection_options: { opt_key: "opt_value" },
            my_key: "my_value"
          }
        ).send(:connection)
      end
    end # describe '#connection'

    describe "#put_object" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:file) { double }

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
        md5_file = double
        expect(Digest::MD5).to receive(:file).with("/src/file").and_return(md5_file)
        expect(md5_file).to receive(:digest).and_return(:md5_digest)
        expect(Base64).to receive(:encode64).with(:md5_digest).and_return("encoded_digest\n")
      end

      it "calls put_object with Content-MD5 header" do
        expect(File).to receive(:open).with("/src/file", "r").and_yield(file)
        expect(connection).to receive(:put_object)
          .with("my_bucket", "dest/file", file, "Content-MD5" => "encoded_digest")
        cloud_io.send(:put_object, "/src/file", "dest/file")
      end

      it "fails after retries" do
        expect(File).to receive(:open).twice.with("/src/file", "r").and_yield(file)
        expect(connection).to receive(:put_object).once
          .with("my_bucket", "dest/file", file, "Content-MD5" => "encoded_digest")
          .and_raise("error1")
        expect(connection).to receive(:put_object).once
          .with("my_bucket", "dest/file", file, "Content-MD5" => "encoded_digest")
          .and_raise("error2")

        expect do
          cloud_io.send(:put_object, "/src/file", "dest/file")
        end.to raise_error CloudIO::Error, "CloudIO::Error: Max Retries (1) Exceeded!\n" \
          "  Operation: PUT 'my_bucket/dest/file'\n" \
          "  Be sure to check the log messages for each retry attempt.\n" \
          "--- Wrapped Exception ---\n" \
          "RuntimeError: error2"
        expect(Logger.messages.map(&:lines).join("\n")).to eq(
          "CloudIO::Error: Retry #1 of 1\n" \
          "  Operation: PUT 'my_bucket/dest/file'\n" \
          "--- Wrapped Exception ---\n" \
          "RuntimeError: error1"
        )
      end

      context "with #encryption and #storage_class set" do
        let(:cloud_io) do
          CloudIO::S3.new(
            bucket: "my_bucket",
            encryption: :aes256,
            storage_class: :reduced_redundancy,
            max_retries: 1,
            retry_waitsec: 0
          )
        end

        it "sets headers for encryption and storage_class" do
          expect(File).to receive(:open).with("/src/file", "r").and_yield(file)
          expect(connection).to receive(:put_object).with(
            "my_bucket", "dest/file", file,
            "Content-MD5" => "encoded_digest",
              "x-amz-server-side-encryption" => "AES256",
              "x-amz-storage-class" => "REDUCED_REDUNDANCY"
          )
          cloud_io.send(:put_object, "/src/file", "dest/file")
        end
      end
    end # describe '#put_object'

    describe "#initiate_multipart" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:response) { double("response", body: { "UploadId" => 1234 }) }

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
        expect(Logger).to receive(:info).with("  Initiate Multipart 'my_bucket/dest/file'")
      end

      it "initiates multipart upload with retries" do
        expect(cloud_io).to receive(:with_retries)
          .with("POST 'my_bucket/dest/file' (Initiate)").and_yield
        expect(connection).to receive(:initiate_multipart_upload)
          .with("my_bucket", "dest/file", {}).and_return(response)

        expect(cloud_io.send(:initiate_multipart, "dest/file")).to be 1234
      end

      context "with #encryption and #storage_class set" do
        let(:cloud_io) do
          CloudIO::S3.new(
            bucket: "my_bucket",
            encryption: :aes256,
            storage_class: :reduced_redundancy,
            max_retries: 1,
            retry_waitsec: 0
          )
        end

        it "sets headers for encryption and storage_class" do
          expect(connection).to receive(:initiate_multipart_upload).with(
            "my_bucket", "dest/file",
            "x-amz-server-side-encryption" => "AES256",
              "x-amz-storage-class" => "REDUCED_REDUNDANCY"
          ).and_return(response)
          expect(cloud_io.send(:initiate_multipart, "dest/file")).to be 1234
        end
      end
    end # describe '#initiate_multipart'

    describe "#upload_parts" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:chunk_bytes) { 1024**2 * 5 }
      let(:file_size) { chunk_bytes + 250 }
      let(:chunk_a) { "a" * chunk_bytes }
      let(:encoded_digest_a) { "ebKBBg0ze5srhMzzkK3PdA==" }
      let(:chunk_a_resp) { double("response", headers: { "ETag" => "chunk_a_etag" }) }
      let(:chunk_b) { "b" * 250 }
      let(:encoded_digest_b) { "OCttLDka1ocamHgkHvZMyQ==" }
      let(:chunk_b_resp) { double("response", headers: { "ETag" => "chunk_b_etag" }) }
      let(:file) { StringIO.new(chunk_a + chunk_b) }

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
      end

      it "uploads chunks with Content-MD5" do
        expect(File).to receive(:open).with("/src/file", "r").and_yield(file)
        allow(StringIO).to receive(:new).with(chunk_a).and_return(:stringio_a)
        allow(StringIO).to receive(:new).with(chunk_b).and_return(:stringio_b)

        expect(cloud_io).to receive(:with_retries).with(
          "PUT 'my_bucket/dest/file' Part #1"
        ).and_yield

        expect(connection).to receive(:upload_part).with(
          "my_bucket", "dest/file", 1234, 1, :stringio_a,
          "Content-MD5" => encoded_digest_a
        ).and_return(chunk_a_resp)

        expect(cloud_io).to receive(:with_retries).with(
          "PUT 'my_bucket/dest/file' Part #2"
        ).and_yield

        expect(connection).to receive(:upload_part).with(
          "my_bucket", "dest/file", 1234, 2, :stringio_b,
          "Content-MD5" => encoded_digest_b
        ).and_return(chunk_b_resp)

        expect(
          cloud_io.send(:upload_parts,
            "/src/file", "dest/file", 1234, chunk_bytes, file_size)
        ).to eq ["chunk_a_etag", "chunk_b_etag"]

        expect(Logger.messages.map(&:lines).join("\n")).to eq(
          "  Uploading 2 Parts...\n" \
          "  ...90% Complete..."
        )
      end

      it "logs progress" do
        chunk_bytes = 1024**2 * 1
        file_size = chunk_bytes * 100
        file = StringIO.new("x" * file_size)
        expect(File).to receive(:open).with("/src/file", "r").and_yield(file)
        allow(Digest::MD5).to receive(:digest)
        allow(Base64).to receive(:encode64).and_return("")
        allow(connection).to receive(:upload_part).and_return(double("response", headers: {}))

        cloud_io.send(:upload_parts,
          "/src/file", "dest/file", 1234, chunk_bytes, file_size)
        expect(Logger.messages.map(&:lines).join("\n")).to eq(
          "  Uploading 100 Parts...\n" \
          "  ...10% Complete...\n" \
          "  ...20% Complete...\n" \
          "  ...30% Complete...\n" \
          "  ...40% Complete...\n" \
          "  ...50% Complete...\n" \
          "  ...60% Complete...\n" \
          "  ...70% Complete...\n" \
          "  ...80% Complete...\n" \
          "  ...90% Complete..."
        )
      end
    end # describe '#upload_parts'

    describe "#upload_stream" do
      # A real OS pipe, not a StringIO. The read loop relies on IO#read(n) blocking until it
      # has n bytes or hits EOF, which is a property of the IO, and a StringIO would satisfy
      # the loop whether or not that held.
      # Bytes the producer has managed to push into the pipe so far. How far it gets ahead of
      # the consumer is the observable signature of backpressure.
      attr_accessor :bytes_written

      def piped(data, chunk: nil)
        self.bytes_written = 0
        reader, writer = IO.pipe
        producer = Thread.new do
          begin
            if chunk
              offset = 0
              while offset < data.bytesize
                slice = data.byteslice(offset, chunk)
                writer.write(slice)
                offset += slice.bytesize
                self.bytes_written = offset
              end
            else
              writer.write(data)
              self.bytes_written = data.bytesize
            end
          rescue Errno::EPIPE
            # The consumer gave up early, which is the point of several of these examples.
            nil
          ensure
            writer.close
          end
        end
        begin
          yield reader
        ensure
          # Close first, then join. On the failure paths the consumer stops reading, and a
          # producer blocked on a full pipe only comes back once the read end is gone.
          reader.close unless reader.closed?
          producer.join
        end
      end

      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          chunk_size: 1,
          upload_concurrency: concurrency,
          tagging: "tier=weekly",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:concurrency) { 1 }
      let(:part_bytes) { 1024**2 }

      before do
        # S3 really does require 5 MiB parts, but that would mean pushing tens of megabytes
        # through a pipe to get enough parts to say anything about ordering or growth. This
        # relaxes S3's rule, not ours: every code path under test is the production one.
        stub_const("Backup::CloudIO::S3::MIN_PART_SIZE", 1024)

        allow(Logger).to receive(:info)
        allow(Logger).to receive(:warn)
        allow(cloud_io).to receive(:connection).and_return(connection)
        allow(cloud_io).to receive(:new_connection).and_return(connection)
        allow(connection).to receive(:initiate_multipart_upload)
          .and_return(double("response", body: { "UploadId" => "upload-1" }))
        allow(connection).to receive(:complete_multipart_upload)
          .and_return(double("response", body: {}))
      end

      # Two things ride on this tag, and the second one is unrecoverable.
      #
      # The retention lifecycle rules keep Mondays a year and everything else 90 days, so an
      # untagged object falls onto the 400-day backstop -- wrong, but recoverable. The
      # cross-account vault replication rule (SQ3-1440) also filters on tier=weekly, and it
      # is evaluated at object creation and never re-fires on a later tag change. Past seven
      # days that vault is the only copy of the database outside eu-west-1, so a Monday that
      # misses the tag is a permanent, silent hole in it.
      #
      # Which is why this is asserted on the InitiateMultipartUpload call specifically:
      # tagging afterwards would pass a get-object-tagging check and still never replicate.
      it "sends x-amz-tagging when initiating the upload" do
        expect(connection).to receive(:initiate_multipart_upload).with(
          "my_bucket", "dest/file", hash_including("x-amz-tagging" => "tier=weekly")
        ).and_return(double("response", body: { "UploadId" => "upload-1" }))
        allow(connection).to receive(:upload_part)
          .and_return(double("response", headers: { "ETag" => "etag" }))

        piped("x") { |io| cloud_io.upload_stream(io, "dest/file") }
      end

      # The streaming and packaged paths must build the same headers, or a change to one
      # silently changes what the other writes.
      it "builds the same headers as the packaged upload path" do
        expect(cloud_io.send(:headers)).to include("x-amz-tagging" => "tier=weekly")
      end

      it "refuses to create an object at all if the tag went missing" do
        allow(cloud_io).to receive(:headers).and_return({})

        expect(connection).to receive(:initiate_multipart_upload).never

        expect do
          piped("x") { |io| cloud_io.upload_stream(io, "dest/file") }
        end.to raise_error(CloudIO::S3::Error, /Object Tagging Lost/)
      end

      it "splits the stream into parts and completes the upload" do
        # Two full parts plus a remainder.
        data = "abcdefghij" * (part_bytes / 10 * 2 + 5)
        sent = []

        allow(connection).to receive(:upload_part) do |_b, _d, _u, number, body, _h|
          sent << [number, body.read.bytesize]
          double("response", headers: { "ETag" => "etag-#{number}" })
        end

        expect(connection).to receive(:complete_multipart_upload)
          .with("my_bucket", "dest/file", "upload-1", %w[etag-1 etag-2 etag-3])
          .and_return(double("response", body: {}))

        piped(data) { |io| cloud_io.upload_stream(io, "dest/file") }

        expect(sent.sort).to eq(
          [[1, part_bytes], [2, part_bytes], [3, data.bytesize - part_bytes * 2]]
        )
      end

      context "with concurrent part uploads" do
        let(:concurrency) { 4 }

        # The strongest statement available without talking to S3: reassemble the object the
        # way S3 would -- concatenating part bodies in the order the ETags were handed to
        # CompleteMultipartUpload -- and require it to be byte-identical to what went in.
        #
        # A transposition here does not raise. It produces an object that uploads cleanly,
        # passes every tagging and object-count check, and is discovered to be garbage at
        # restore time, which is the worst moment to discover it.
        it "assembles byte-identically to the input when parts complete out of order" do
          data = (1..8).map { |n| n.to_s * part_bytes }.join
          bodies = {}

          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, body, _h|
            bodies["etag-#{number}"] = body.read
            # Later parts finish first, so append order and part order disagree.
            sleep((9 - number) * 0.01)
            double("response", headers: { "ETag" => "etag-#{number}" })
          end

          completed = nil
          allow(connection).to receive(:complete_multipart_upload) do |_b, _d, _u, etags|
            completed = etags
            double("response", body: {})
          end

          piped(data) { |io| cloud_io.upload_stream(io, "dest/file") }

          expect(completed.map { |etag| bodies.fetch(etag) }.join).to eq(data)
        end

        # Parts are acknowledged in whatever order the uploads finish, but
        # CompleteMultipartUpload needs them in part order or S3 assembles the object wrong.
        it "completes with the ETags in part order regardless of completion order" do
          data = "z" * (part_bytes * 8)

          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, _body, _h|
            # Later parts finish first.
            sleep((9 - number) * 0.01)
            double("response", headers: { "ETag" => "etag-#{number}" })
          end

          expect(connection).to receive(:complete_multipart_upload)
            .with(
              "my_bucket", "dest/file", "upload-1", (1..8).map { |n| "etag-#{n}" }
            )
            .and_return(double("response", body: {}))

          piped(data) { |io| cloud_io.upload_stream(io, "dest/file") }
        end

        # The memory claim on Storage::S3#upload_concurrency is that roughly
        # 2 x concurrency x chunk_size is in flight. That rests entirely on the queue being
        # a SizedQueue, so this asserts the bound where it can actually be seen: how far the
        # producer gets ahead while the uploads are slow.
        #
        # Counting parts inside #upload_part does NOT test this -- the worker count caps that
        # number whether the queue is bounded or not, so the assertion passes either way.
        # With a bounded queue the reader stops pushing and the producer blocks on a full
        # pipe; with an unbounded one the reader swallows the entire stream into memory
        # immediately and the producer runs to completion.
        it "stops reading the stream when the uploads fall behind" do
          total_parts = 40
          seen = 0
          progress = nil
          counter = Mutex.new

          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, _body, _h|
            counter.synchronize do
              seen += 1
              progress ||= bytes_written if seen == 5
            end
            sleep 0.02 # slower than the reader can produce parts
            double("response", headers: { "ETag" => "etag-#{number}" })
          end

          data = "z" * (part_bytes * total_parts)
          piped(data, chunk: part_bytes) do |io|
            cloud_io.upload_stream(io, "dest/file")
          end

          # By the fifth part the producer must still have most of the stream to send.
          #
          # The theoretical bound is (2 x concurrency + 1) parts -- the queue, the workers,
          # and the one being read -- which measured 9 MiB of the 40. Unbounded measured
          # 31 MiB, so half the stream sits well clear of both.
          expect(progress).to be < data.bytesize / 2
        end

        it "gives each worker its own connection, since fog is not thread-safe" do
          allow(connection).to receive(:upload_part)
            .and_return(double("response", headers: { "ETag" => "etag" }))

          expect(cloud_io).to receive(:new_connection).exactly(4).times
            .and_return(connection)

          piped("x" * part_bytes) { |io| cloud_io.upload_stream(io, "dest/file") }
        end

        # An exception raised inside a Thread is swallowed unless someone looks for it. The
        # dangerous outcome is not a crash, it is an upload that completes with a part
        # missing and is only found to be broken at restore.
        it "never completes the upload when a worker loses a part" do
          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, _body, _h|
            raise "part #{number} failed" if number == 5
            double("response", headers: { "ETag" => "etag-#{number}" })
          end
          allow(connection).to receive(:abort_multipart_upload)

          expect(connection).to receive(:complete_multipart_upload).never

          expect do
            Timeout.timeout(30) do
              piped("z" * (part_bytes * 8)) { |io| cloud_io.upload_stream(io, "dest/file") }
            end
          end.to raise_error(CloudIO::Error, /part 5 failed/)
        end

        it "aborts and re-raises when a part fails, without hanging" do
          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, _body, _h|
            raise "part #{number} failed" if number == 2
            double("response", headers: { "ETag" => "etag-#{number}" })
          end

          expect(connection).to receive(:abort_multipart_upload)
            .with("my_bucket", "dest/file", "upload-1")
          expect(connection).to receive(:complete_multipart_upload).never

          expect do
            Timeout.timeout(30) do
              piped("z" * (part_bytes * 8)) { |io| cloud_io.upload_stream(io, "dest/file") }
            end
          end.to raise_error(CloudIO::Error, /part 2 failed/)
        end
      end

      it "aborts the upload when completing it fails, leaving no object behind" do
        allow(connection).to receive(:upload_part)
          .and_return(double("response", headers: { "ETag" => "etag" }))
        allow(connection).to receive(:complete_multipart_upload).and_raise("complete failed")

        expect(connection).to receive(:abort_multipart_upload)
          .with("my_bucket", "dest/file", "upload-1")

        expect do
          piped("x") { |io| cloud_io.upload_stream(io, "dest/file") }
        end.to raise_error(/complete failed/)
      end

      it "does not mask the real error when the abort itself fails" do
        allow(connection).to receive(:upload_part).and_raise("part failed")
        allow(connection).to receive(:abort_multipart_upload).and_raise("abort failed")

        expect do
          piped("x") { |io| cloud_io.upload_stream(io, "dest/file") }
        end.to raise_error(CloudIO::Error, /part failed/)
      end

      # A stream has no size to plan against, so the part size grows rather than the upload
      # dying at part 10,001 after hours of work.
      it "grows the part size once the stream passes the growth threshold" do
        stub_const("Backup::CloudIO::S3::PART_SIZE_GROWTH_AFTER", 2)
        stub_const("Backup::CloudIO::S3::MAX_STREAM_PART_SIZE", part_bytes * 2)

        sizes = []
        allow(connection).to receive(:upload_part) do |_b, _d, _u, number, body, _h|
          sizes << body.read.bytesize
          double("response", headers: { "ETag" => "etag-#{number}" })
        end

        piped("z" * (part_bytes * 6)) { |io| cloud_io.upload_stream(io, "dest/file") }

        # Two parts at 1 MiB, then the size doubles for the rest.
        expect(sizes.first(2)).to eq([part_bytes, part_bytes])
        expect(sizes[2]).to eq(part_bytes * 2)
        expect(sizes.sum).to eq(part_bytes * 6)
      end

      it "refuses a stream that needs more parts than S3 allows" do
        stub_const("Backup::CloudIO::S3::MAX_PARTS", 2)
        stub_const("Backup::CloudIO::S3::PART_SIZE_GROWTH_AFTER", 1_000)
        allow(connection).to receive(:upload_part)
          .and_return(double("response", headers: { "ETag" => "etag" }))
        allow(connection).to receive(:abort_multipart_upload)

        expect do
          piped("z" * (part_bytes * 4)) { |io| cloud_io.upload_stream(io, "dest/file") }
        end.to raise_error(CloudIO::S3::Error, /Stream Too Large/)
      end

      context "when #chunk_size cannot be used for a stream" do
        let(:cloud_io) do
          CloudIO::S3.new(
            bucket: "my_bucket",
            chunk_size: 0, # multipart disabled, which a stream cannot honour
            upload_concurrency: 1,
            max_retries: 1,
            retry_waitsec: 0
          )
        end

        it "falls back to the default stream part size" do
          sizes = []
          allow(connection).to receive(:upload_part) do |_b, _d, _u, number, body, _h|
            sizes << body.read.bytesize
            double("response", headers: { "ETag" => "etag-#{number}" })
          end

          piped("z" * 1024) { |io| cloud_io.upload_stream(io, "dest/file") }

          # One short part, because the data ran out well before 64 MiB.
          expect(sizes).to eq([1024])
        end
      end
    end # describe '#upload_stream'

    describe "#complete_multipart" do
      let(:cloud_io) do
        CloudIO::S3.new(
          bucket: "my_bucket",
          max_retries: 1,
          retry_waitsec: 0
        )
      end
      let(:resp_ok) do
        double(
          "response",
          body: {
            "Location" => "http://my_bucket.s3.amazonaws.com/dest/file",
            "Bucket" => "my_bucket",
            "Key" => "dest/file",
            "ETag" => '"some-etag"'
          }
        )
      end
      let(:resp_bad) do
        double(
          "response",
          body: {
            "Code" => "InternalError",
            "Message" => "We encountered an internal error. Please try again."
          }
        )
      end

      before do
        allow(cloud_io).to receive(:connection).and_return(connection)
      end

      it "retries on raised errors" do
        expect(connection).to receive(:complete_multipart_upload).once
          .with("my_bucket", "dest/file", 1234, [:parts])
          .and_raise("error")
        expect(connection).to receive(:complete_multipart_upload).once
          .with("my_bucket", "dest/file", 1234, [:parts])
          .and_return(resp_ok)
        cloud_io.send(:complete_multipart, "dest/file", 1234, [:parts])
      end

      it "retries on returned errors" do
        expect(connection).to receive(:complete_multipart_upload).twice
          .with("my_bucket", "dest/file", 1234, [:parts])
          .and_return(resp_bad, resp_ok)
        cloud_io.send(:complete_multipart, "dest/file", 1234, [:parts])
      end

      it "fails after retries exceeded" do
        expect(connection).to receive(:complete_multipart_upload).once
          .with("my_bucket", "dest/file", 1234, [:parts])
          .and_raise("error message")
        expect(connection).to receive(:complete_multipart_upload).once
          .with("my_bucket", "dest/file", 1234, [:parts])
          .and_return(resp_bad)

        expect do
          cloud_io.send(:complete_multipart, "dest/file", 1234, [:parts])
        end.to raise_error CloudIO::Error, "CloudIO::Error: Max Retries (1) Exceeded!\n" \
          "  Operation: POST 'my_bucket/dest/file' (Complete)\n" \
          "  Be sure to check the log messages for each retry attempt.\n" \
          "--- Wrapped Exception ---\n" \
          "CloudIO::S3::Error: The server returned the following error:\n" \
          "  InternalError: We encountered an internal error. Please try again."
        expect(Logger.messages.map(&:lines).join("\n")).to eq(
          "  Complete Multipart 'my_bucket/dest/file'\n" \
          "CloudIO::Error: Retry #1 of 1\n" \
          "  Operation: POST 'my_bucket/dest/file' (Complete)\n" \
          "--- Wrapped Exception ---\n" \
          "RuntimeError: error message"
        )
      end
    end # describe '#complete_multipart'

    describe "#headers" do
      let(:cloud_io) { CloudIO::S3.new }

      it "returns empty headers by default" do
        allow(cloud_io).to receive(:encryption).and_return(nil)
        allow(cloud_io).to receive(:storage_class).and_return(nil)
        expect(cloud_io.send(:headers)).to eq({})
      end

      it "returns headers for server-side encryption" do
        allow(cloud_io).to receive(:storage_class).and_return(nil)
        ["aes256", :aes256].each do |arg|
          allow(cloud_io).to receive(:encryption).and_return(arg)
          expect(cloud_io.send(:headers)).to eq(
            "x-amz-server-side-encryption" => "AES256"
          )
        end
      end

      it "returns headers for reduced redundancy storage" do
        allow(cloud_io).to receive(:encryption).and_return(nil)
        ["reduced_redundancy", :reduced_redundancy].each do |arg|
          allow(cloud_io).to receive(:storage_class).and_return(arg)
          expect(cloud_io.send(:headers)).to eq(
            "x-amz-storage-class" => "REDUCED_REDUNDANCY"
          )
        end
      end

      it "returns headers for both" do
        allow(cloud_io).to receive(:encryption).and_return(:aes256)
        allow(cloud_io).to receive(:storage_class).and_return(:reduced_redundancy)
        expect(cloud_io.send(:headers)).to eq(
          "x-amz-server-side-encryption" => "AES256",
            "x-amz-storage-class" => "REDUCED_REDUNDANCY"
        )
      end

      it "returns empty headers for empty values" do
        allow(cloud_io).to receive(:encryption).and_return("")
        allow(cloud_io).to receive(:storage_class).and_return("")
        expect(cloud_io.send(:headers)).to eq({})
      end

      it "returns headers for object tagging" do
        allow(cloud_io).to receive(:encryption).and_return(nil)
        allow(cloud_io).to receive(:storage_class).and_return(nil)
        allow(cloud_io).to receive(:tagging).and_return("tier=weekly")
        expect(cloud_io.send(:headers)).to eq(
          "x-amz-tagging" => "tier=weekly"
        )
      end

      it "passes a multi-tag query string through verbatim" do
        allow(cloud_io).to receive(:encryption).and_return(nil)
        allow(cloud_io).to receive(:storage_class).and_return(nil)
        allow(cloud_io).to receive(:tagging).and_return("tier=weekly&source=mongo")
        expect(cloud_io.send(:headers)).to eq(
          "x-amz-tagging" => "tier=weekly&source=mongo"
        )
      end

      it "returns headers for encryption, storage class and tagging together" do
        allow(cloud_io).to receive(:encryption).and_return(:aes256)
        allow(cloud_io).to receive(:storage_class).and_return(:reduced_redundancy)
        allow(cloud_io).to receive(:tagging).and_return("tier=daily")
        expect(cloud_io.send(:headers)).to eq(
          "x-amz-server-side-encryption" => "AES256",
          "x-amz-storage-class" => "REDUCED_REDUNDANCY",
          "x-amz-tagging" => "tier=daily"
        )
      end

      it "omits the tagging header for empty and nil values" do
        allow(cloud_io).to receive(:encryption).and_return(nil)
        allow(cloud_io).to receive(:storage_class).and_return(nil)
        [nil, ""].each do |arg|
          allow(cloud_io).to receive(:tagging).and_return(arg)
          expect(cloud_io.send(:headers)).to eq({})
        end
      end
    end # describe '#headers

    describe "Object" do
      let(:cloud_io) { CloudIO::S3.new }
      let(:obj_data) do
        { "Key" => "obj_key", "ETag" => "obj_etag", "StorageClass" => "STANDARD" }
      end
      let(:object) { CloudIO::S3::Object.new(cloud_io, obj_data) }

      describe "#initialize" do
        it "creates Object from data" do
          expect(object.key).to eq "obj_key"
          expect(object.etag).to eq "obj_etag"
          expect(object.storage_class).to eq "STANDARD"
        end
      end

      describe "#encryption" do
        it "returns the algorithm used for server-side encryption" do
          expect(cloud_io).to receive(:head_object).once.with(object).and_return(
            double("response", headers: { "x-amz-server-side-encryption" => "AES256" })
          )
          expect(object.encryption).to eq "AES256"
          expect(object.encryption).to eq "AES256"
        end

        it "returns nil if SSE was not used" do
          expect(cloud_io).to receive(:head_object).once.with(object)
            .and_return(double("response", headers: {}))
          expect(object.encryption).to be_nil
          expect(object.encryption).to be_nil
        end
      end # describe '#encryption'
    end # describe 'Object'
  end
end
