cmake_minimum_required(VERSION 3.6)
project(membership_protocol)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        mp1/Application.cpp
        mp1/Application.h
        mp1/EmulNet.cpp
        mp1/EmulNet.h
        mp1/Log.cpp
        mp1/Log.h
        mp1/Member.cpp
        mp1/Member.h
        mp1/MP1Node.cpp
        mp1/MP1Node.h
        mp1/Params.cpp
        mp1/Params.h
        mp1/Queue.h
        mp1/stdincludes.h)

add_executable(membership_protocol ${SOURCE_FILES})