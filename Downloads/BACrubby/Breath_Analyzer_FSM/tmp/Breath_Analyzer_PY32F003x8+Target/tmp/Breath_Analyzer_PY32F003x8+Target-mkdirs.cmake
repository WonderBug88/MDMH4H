# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/1"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/tmp"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/src/Breath_Analyzer_PY32F003x8+Target-stamp"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/src"
  "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/src/Breath_Analyzer_PY32F003x8+Target-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/src/Breath_Analyzer_PY32F003x8+Target-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/amr/myworkspace/Breath_Analyzer_FSM/tmp/Breath_Analyzer_PY32F003x8+Target/src/Breath_Analyzer_PY32F003x8+Target-stamp${cfgdir}") # cfgdir has leading slash
endif()
