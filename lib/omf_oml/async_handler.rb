#-------------------------------------------------------------------------------
# Copyright (c) 2015 National ICT Australia Limited (NICTA).
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'omf_base/lobject'

module OMF::OML

  class AsyncHandler < OMF::Base::LObject

    def onSuccess(&block)
      if @success
        block.call @success
      else
        raise "Only one handler can be registered" if @successHandler
        @successHandler = block
      end
      self
    end

    def onFailure(&block)
      if @failure
        block.call @failure
      else
        raise "Only one handler can be registered" if @failureHandler
        @failureHandler = block
      end
      self
    end

    def success(success)
      if @successHandler
        @successHandler.call(success)
      else
        @success = success
      end
    end

    def failure(success)
      if @failureHandler
        @failureHandler.call(success)
      else
        @failure = success
      end
    end


  end # class
end # module
