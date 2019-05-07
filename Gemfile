source 'https://rubygems.org'
git_source(:github) { |repo| "https://github.com/#{repo}.git" }

ruby '2.6.1'

gem 'activerecord-import'
gem 'bootsnap', '>= 1.1.0', require: false
gem 'oj'
gem 'pg', '>= 0.18', '< 2.0'
gem 'puma', '~> 3.11'
gem 'progressbar'
gem 'rails', '~> 5.2.3'
gem 'strong_migrations'

group :development, :test do
  # Call 'byebug' anywhere in the code to stop execution and get a debugger console
  gem 'byebug', platforms: [:mri, :mingw, :x64_mingw]
end

group :development do
  # Access an interactive console on exception pages or by calling 'console' anywhere in the code.
  gem 'web-console', '>= 3.3.0'
  gem 'listen', '>= 3.0.5', '< 3.2'

  # Profiling
  gem 'rack-mini-profiler', require: false
  gem 'memory_profiler'
  gem 'meta_request'
  gem 'flamegraph'
  gem 'stackprof'

  # Optimization
  gem 'bullet'
  gem 'pghero'
end

group :development, :test do
  gem 'rspec-rails', '~> 3.8'
end

# Windows does not include zoneinfo files, so bundle the tzinfo-data gem
gem 'tzinfo-data', platforms: [:mingw, :mswin, :x64_mingw, :jruby]
