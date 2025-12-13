# frozen_string_literal: true

require File.expand_path("lib/backup/version")

Gem::Specification.new do |gem|
  gem.name        = "backup"
  gem.version     = Backup::VERSION
  gem.authors     = "Michael van Rooijen"
  gem.email       = "meskyanichi@gmail.com"
  gem.homepage    = "https://github.com/backup/backup"
  gem.license     = "MIT"
  gem.summary     = "Provides an elegant DSL in Ruby for performing backups " \
                    "on UNIX-like systems."
  gem.description = <<-DESCRIPTION.gsub(/\s+/, " ").strip
    Backup is a RubyGem, written for UNIX-like operating systems, that allows
    you to easily perform backup operations on both your remote and local
    environments. It provides you with an elegant DSL in Ruby for modeling your
    backups.  Backup has built-in support for various databases, storage
    protocols/services, syncers, compressors, encryptors and notifiers which
    you can mix and match. It was built with modularity, extensibility and
    simplicity in mind.
  DESCRIPTION

  gem.files = `git ls-files -- lib bin templates README.md LICENSE`.split("\n")
  gem.require_path  = "lib"
  gem.executables   = ["backup"]

  gem.required_ruby_version = Gem::Requirement.new(">= 2.7.0")

  gem.add_dependency "activesupport", "~> 6"
  gem.add_dependency "aws-sdk", "~> 2"
  gem.add_dependency "dogapi", "1.40.0"
  gem.add_dependency "dropbox-sdk", "1.6.5"
  gem.add_dependency "excon", "~> 0.71"
  gem.add_dependency "flowdock", "0.4.0"
  gem.add_dependency "fog", "~> 1.42"
  gem.add_dependency "hipchat", "1.0.1"
  gem.add_dependency "mail"
  gem.add_dependency "net-ftp"
  gem.add_dependency "net-scp"
  gem.add_dependency "net-sftp"
  gem.add_dependency "net-smtp"
  gem.add_dependency "net-ssh"
  gem.add_dependency "nokogiri"
  gem.add_dependency "open4"
  gem.add_dependency "pagerduty", "2.0.0"
  gem.add_dependency "qiniu"
  gem.add_dependency "thor"
  gem.add_dependency "twitter", "~> 6.0"
  gem.add_dependency "unf", "0.1.3" # for fog/AWS

  gem.add_development_dependency "rake"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "rubocop"
  gem.add_development_dependency "timecop"
  gem.metadata["rubygems_mfa_required"] = "true"
end
