/*
** Copyright 2019 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
#ifndef BLOOMBERG_COROKAFKA_THROTTLE_CONTROL_H
#define BLOOMBERG_COROKAFKA_THROTTLE_CONTROL_H

namespace Bloomberg {
namespace corokafka {
            
struct ThrottleControl
{
    enum class Status : char
    {
        On = 0, //throttle was off and it's turned on
        Off,    //throttle was on and it's turned off
        Unchanged //throttle is still ongoing or was never turned on. This is a no-op
    };
    
    Status handleThrottleCallback(std::chrono::milliseconds throttleDuration)
    {
        Status status = Status::Unchanged;
        if (_autoThrottle) {
            quantum::Mutex::Guard guard(_throttleMutex);
            if (isThrottleOn(throttleDuration)) {
                status = Status::On;
            }
            else if (isThrottleOff(throttleDuration)) {
                status = Status::Off;
            }
            _throttleDuration = throttleDuration * _throttleMultiplier;
            _throttleTime = std::chrono::steady_clock::now();
        }
        return status;
    }
    
    bool reduceThrottling(const std::chrono::steady_clock::time_point& currentTime)
    {
        if (_autoThrottle) {
            quantum::Mutex::Guard guard(_throttleMutex);
            if (_throttleDuration > std::chrono::milliseconds::zero()) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - _throttleTime);
                if (elapsed > _throttleDuration) {
                    //throttle time elapsed so we reset the duration
                    _throttleDuration = std::chrono::milliseconds::zero();
                    return true;
                }
            }
        }
        return false;
    }
    
    void reset() {
        quantum::Mutex::Guard guard(_throttleMutex);
        _throttleDuration = std::chrono::milliseconds::zero();
        _throttleTime = std::chrono::steady_clock::time_point{};
    }
    
    bool& autoThrottle() { return _autoThrottle; }
    uint16_t& throttleMultiplier() { return _throttleMultiplier; }
    
private:
    bool isThrottleOn(std::chrono::milliseconds throttleDuration) const {
        return (_throttleDuration.count() == 0) && (throttleDuration.count() > 0);
    }
    bool isThrottleOff(std::chrono::milliseconds throttleDuration) const {
        return (_throttleDuration.count() > 0) && (throttleDuration.count() == 0);
    }
    quantum::Mutex                          _throttleMutex;
    bool                                    _autoThrottle{false};
    uint16_t                                _throttleMultiplier{1};
    std::chrono::steady_clock::time_point   _throttleTime;
    std::chrono::milliseconds               _throttleDuration{0};
};

}}

#endif //BLOOMBERG_COROKAFKA_THROTTLE_CONTROL_H
