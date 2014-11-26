# A rakefile for building EventStore.Client.FSharp

require 'bundler/setup'

require 'albacore'
require 'albacore/tasks/release'
require 'albacore/tasks/versionizer'
require 'albacore/ext/teamcity'

Configuration = 'Release'

Albacore::Tasks::Versionizer.new :versioning

desc 'create assembly infos'
asmver_files :assembly_info do |a|
  a.files = FileList['**/*proj'] # optional, will find all projects recursively by default

  a.attributes assembly_description: 'EventStore.Client.FSharp is a NuGet that adds a F#-ideomatic interface on top of EventStore',
               assembly_configuration: Configuration,
               assembly_company: 'Logibit AB',
               assembly_copyright: "(c) #{Time.now.year} by Henrik Feldt",
               assembly_version: ENV['LONG_VERSION'],
               assembly_file_version: ENV['LONG_VERSION'],
               assembly_informational_version: ENV['BUILD_VERSION']
end

desc 'Perform fast build (warn: doesn\'t d/l deps)'
build :quick_compile do |b|
  b.logging = 'detailed'
  b.sln     = 'src/EventStore.Client.FSharp.sln'
end

task :paket_bootstrap do
  system 'tools/paket.bootstrapper.exe', clr_command: true unless \
    File.exists? 'tools/paket.exe'
end

desc 'restore all nugets as per the packages.config files'
task :restore => :paket_bootstrap do
  system 'tools/paket.exe', 'restore', clr_command: true
end

desc 'Perform full build'
build :compile => [:versioning, :restore, :assembly_info] do |b|
  b.sln = 'src/EventStore.Client.FSharp.sln'
end

directory 'build/pkg'

desc 'package nugets - finds all projects and package them'
nugets_pack :create_nugets => ['build/pkg', :versioning, :compile] do |p|
  p.files   = FileList['src/**/*.{csproj,fsproj,nuspec}'].
    exclude(/Tests/)
  p.out     = 'build/pkg'
  p.exe     = 'packages/NuGet.CommandLine/tools/NuGet.exe'
  p.with_metadata do |m|
    m.id          = 'EventStore.Client.FSharp'
    m.title       = 'EventStore.Client F# API'
    m.description = 'EventStore.Client.FSharp is a NuGet that adds a F#-ideomatic interface on top of EventStore'
    m.authors     = 'Henrik Feldt, Logibit AB'
    m.project_url = 'https://github.com/haf/EventStore.Client.FSharp'
    m.tags        = 'F# fsharp EventStore eventsourcing eventsource ddd aggregates stream'
    m.version     = ENV['NUGET_VERSION']
  end
end

namespace :tests do
  task :unit_quick do
    system 'src/EventStore.ClientAPI.FSharp.Tests/bin/Debug/EventStore.ClientAPI.FSharp.Tests.exe',
           clr_command: true
  end

  desc 'run all unit tests'
  task :unit => [:compile, :unit_quick]
end

desc 'run all tests'
task :tests => :'tests:unit'

desc 'run tests and create nuget'
task :default => [:tests, :create_nugets]

task :ensure_nuget_key do
  raise 'missing env NUGET_KEY value' unless ENV['NUGET_KEY']
end

Albacore::Tasks::Release.new :release,
                             pkg_dir: 'build/pkg',
                             depend_on: [:create_nugets, :ensure_nuget_key],
                             nuget_exe: 'packages/NuGet.CommandLine/tools/NuGet.exe',
                             api_key: ENV['NUGET_KEY']
