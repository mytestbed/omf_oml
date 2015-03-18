#-------------------------------------------------------------------------------
# Copyright (c) 2015 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_base/lobject'

module OMF::OML

  class AsyncHandler < OMF::Base::LObject

    def on_success(&block)
      if @success
        _call @success, block
      else
        raise "Only one handler can be registered" if @successHandler
        @successHandler = block
      end
      self
    end

    def on_failure(&block)
      if @failure
        _call @failure, block
      else
        raise "Only one handler can be registered" if @failureHandler
        @failureHandler = block
      end
      self
    end

    def success(success)
      if @successHandler
        _call success, @successHandler
      else
        @success = success
      end
    end

    def failure(failure)
      if @failureHandler
        _call failure, @failureHandler
      else
        @failure = failure
      end
    end

    def chain(other)
      other.on_success do |s|
        success(s)
      end
      other.on_failure do |f|
        failure(f)
      end
    end

    def _call(arg, block)
      EM.next_tick do
        begin
          block.call(arg)
        rescue Exception => ex
          warn "Caught exception in event handler - #{ex}"
          debug ex.backtrace.join("\n\t")
        end
      end
    end


  end # class
end # module
