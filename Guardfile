# A very hacky first attemt at a GUnit guard
# TODO: guard a cmakelist
# TODO: add better messages to guard

require 'active_support/inflector'
require 'guard/guard'


module ::Guard
  class GTest < Guard

    def initialize(w = [], options={})
      super
      @options = {
                :all_on_start   => true,
                :all_after_pass => true,
                :keep_failed    => true,
                :auto_make       => true,
                :test_paths      => 'bin/test'
      }.update(options)


      @last_failed  = false
      @failed_paths = []
    end

    def start
      run_all if @options[:all_on_start]
    end

    def run_all
      paths = _test_paths

      ::Guard::UI.info(options[:message] || "Running: #{paths.join(' ')}", :reset => true)

      return true if paths.empty?

      passed = _run(paths)

      @failed_paths = [] if passed
      @last_failed  = !passed
    end

    def reload
      @failed_paths = []
    end

    def run_on_change(paths)
      paths = (paths + @failed_paths).uniq if @options[:keep_failed]
      passed = _run(paths)

      if passed
        # clean failed paths memory
        @failed_paths -= paths if @options[:keep_failed]

        # run all the tests if the changed tests failed, like autotest
        run_all if @last_failed && @options[:all_after_pass]
      else
        # remember failed paths for the next change
        @failed_paths += paths if @options[:keep_failed]

        # track whether the changed tests failed for the next change
        @last_failed = true
      end
    end

    private
    def _run(paths)
      paths = paths.compact.uniq

      ::Guard::UI.info("Running1: #{paths.join(' ')}", :reset => true)
      status = paths.map do |p|

        # We add the automake here since we have no master makefile
        # TODO: Add this as an option
        if @options[:auto_make]
          make_command = "cd #{File.dirname(p)}; make"
          unless system(make_command)
            ::Guard::Notifier.notify(
                                     "Make failed!", #TODO: Add why it failed
                                     :title => "Make Failed",
                                     :image =>  :failed
                                     )
            return false
          end
        end



        [system('./'+p), p] if File.exists? p
      end
      failed_tests = status.compact.select{|x| !x[0]}.map{|x| x[1]}
      if !failed_tests.empty?
        ::Guard::Notifier.notify(
                                 "Test #{failed_tests.join(' ')} failed!", #TODO: Add why it failed
                                 :title => "Test Failed",
                                 :image =>  :failed
                                 )
      else #TODO: Move this somewhere else
        ::Guard::Notifier.notify(
                                 "Suceeded!", #TODO: Add why/what worked
                                 :title => "Test Passed",
                                 :image =>  :success
                                 )
      end


      true
    end

    def _test_paths
      paths = []
      @options[:test_paths].each do |test_path|
        paths += (Dir[File.join(test_path, '**', '*test')])
      end
      @paths ||= paths.compact.uniq
    end
  end
end

guard 'gtest', :test_paths => ['rm', 'pf'] do
  # Uncomment this line if you have a flat compile system
  #watch(%r{(((?!\/).)+)\.(h|c|hxx|cxx|cpp)$}) { |m|
  watch(%r{(.+)\.(h|cc|c|hxx|cxx|cpp)$}) { |m|
 #   name = m[1].split('/')
    name = m[1]

    puts name

    if name =~ /test$/
      "#{name}"
    else
      "#{name}test"
    end
  }

  # Enable this if not using automake
  # watch(%r{(.+)_test$}) {|m| "#{m[1]}_test" }

  # Enable this if using automake The top one covers this case
  #watch(%r{(((?!\/).)+_test)\.cxx$}) {|m| "bin/test/#{m[1]}"}
end
