# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "omf-oml/version"

Gem::Specification.new do |s|
  s.name        = "omf-oml"
#  s.version     = OmfWeb::VERSION
  s.version     = '0.9.0'
  s.authors     = ["NICTA"]
  s.email       = ["omf-user@lists.nicta.com.au"]
  s.homepage    = "https://www.mytestbed.net"
  s.summary     = %q{Glue between OMF and OML.}
  s.description = %q{Glue functionality between OMF and OMF related libraries, such as OMF Web and Labwiki, and
    OML.}

  s.rubyforge_project = "omf-oml"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
#  s.add_development_dependency "minitest", "~> 2.11.3"
  s.add_runtime_dependency "sqlite3", "~> 1.3.6"
end
