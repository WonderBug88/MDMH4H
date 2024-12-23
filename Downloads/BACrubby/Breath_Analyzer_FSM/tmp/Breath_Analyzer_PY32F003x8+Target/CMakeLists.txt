cmake_minimum_required(VERSION 3.27)

# Roots
include("../roots.cmake")

set(CONTEXT Breath_Analyzer_PY32F003x8+Target)
set(TARGET ${CONTEXT})
set(OUT_DIR "${SOLUTION_ROOT}/out/Breath_Analyzer_PY32F003x8/Target")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(LD_SCRIPT "${SOLUTION_ROOT}/RTE/Device/PY32F003x8/ac6_linker_script.sct.src")
set(LD_REGIONS "${SOLUTION_ROOT}/RTE/Device/PY32F003x8/regions_PY32F003x8.h")
set(LD_SCRIPT_PP "${CMAKE_CURRENT_BINARY_DIR}/ac6_linker_script.sct")

# Processor Options
set(CPU Cortex-M0+)
set(FPU NO_FPU)
set(BYTE_ORDER Little-endian)

# Toolchain config map
include("toolchain.cmake")

# Setup project
project(${CONTEXT} LANGUAGES C ASM)

# Compilation database
add_custom_target(database
  COMMAND ${CMAKE_COMMAND} -E make_directory "${OUT_DIR}"
  COMMAND ${CMAKE_COMMAND} -E copy_if_different "${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json" "${OUT_DIR}"
)
set(CMAKE_C_STANDARD_INCLUDE_DIRECTORIES ${CMAKE_C_IMPLICIT_INCLUDE_DIRECTORIES})

# Setup context
add_executable(${CONTEXT})
set_target_properties(${CONTEXT} PROPERTIES PREFIX "" SUFFIX ".axf" OUTPUT_NAME "Breath_Analyzer_PY32F003x8")
set_target_properties(${CONTEXT} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${OUT_DIR})
add_library(${CONTEXT}_GLOBAL INTERFACE)

# Includes
target_include_directories(${CONTEXT} PUBLIC
  ${SOLUTION_ROOT}/RTE/_Target
  ${CMSIS_PACK_ROOT}/ARM/CMSIS/6.1.0/CMSIS/Core/Include
  ${CMSIS_PACK_ROOT}/Puya/PY32F0xx_DFP/1.2.2/Drivers/CMSIS/Device/PY32F0xx/Include
  ${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Inc
  ${SOLUTION_ROOT}/Inc
)

# Defines
target_compile_definitions(${CONTEXT} PUBLIC
  $<$<COMPILE_LANGUAGE:C,CXX>:
    USE_HAL_DRIVER
    PY32F003x8
    _RTE_
  >
)

# Compile options
target_compile_options(${CONTEXT} PUBLIC
  $<$<COMPILE_LANGUAGE:ASM>:
    "SHELL:${ASM_CPU}"
    "SHELL:${ASM_FLAGS}"
    "SHELL:${ASM_BYTE_ORDER}"
    "SHELL:-masm=auto"
  >
  $<$<COMPILE_LANGUAGE:C>:
    "SHELL:${CC_CPU}"
    "SHELL:${CC_FLAGS}"
    "SHELL:${CC_BYTE_ORDER}"
    "SHELL:-std=c99"
    "SHELL:-fno-rtti"
    "SHELL:-funsigned-char"
    "SHELL:-fshort-enums"
    "SHELL:-fshort-wchar"
    "SHELL:-ffunction-sections"
    "SHELL:-Wall"
    "SHELL:-Wextra"
    "SHELL:-Wno-packed"
    "SHELL:-Wno-reserved-id-macro"
    "SHELL:-Wno-unused-macros"
    "SHELL:-Wno-documentation-unknown-command"
    "SHELL:-Wno-documentation"
    "SHELL:-Wno-license-management"
    "SHELL:-Wno-parentheses-equality"
  >
)

# Compile Options Abstractions
add_library(${CONTEXT}_ABSTRACTIONS INTERFACE)
cbuild_set_options_flags(CC "debug" "on" "" "" CC_OPTIONS_FLAGS_${CONTEXT})
cbuild_set_options_flags(ASM "debug" "on" "" "" ASM_OPTIONS_FLAGS_${CONTEXT})
target_compile_options(${CONTEXT}_ABSTRACTIONS INTERFACE
  $<$<COMPILE_LANGUAGE:C>:
    "SHELL:${CC_OPTIONS_FLAGS_${CONTEXT}}"
  >
  $<$<COMPILE_LANGUAGE:ASM>:
    "SHELL:${ASM_OPTIONS_FLAGS_${CONTEXT}}"
  >
)

# Add groups and components
include("groups.cmake")
include("components.cmake")

target_link_libraries(${CONTEXT} PUBLIC
  Group_Src
  Group_HAL_Drivers_Inc
  Group_HAL_Drivers_Src
  Group_Inc
  ARM_CMSIS_CORE_6_1_0
  Puya_Device_Startup_1_0_0
)

# Linker options
target_link_options(${CONTEXT} PUBLIC
  "SHELL:${LD_CPU}"
  "SHELL:${_LS}\"${LD_SCRIPT_PP}\""
  "SHELL:--map"
  "SHELL:--load_addr_map_info"
  "SHELL:--xref"
  "SHELL:--callgraph"
  "SHELL:--symbols"
  "SHELL:--info sizes"
  "SHELL:--info totals"
  "SHELL:--info unused"
  "SHELL:--info veneers"
  "SHELL:--strict"
  "SHELL:--summary_stderr"
  "SHELL:--info summarysizes"
)
set_target_properties(${CONTEXT} PROPERTIES LINK_DEPENDS ${LD_SCRIPT})

# Linker script pre-processing
add_custom_command(TARGET ${CONTEXT} PRE_LINK COMMAND ${CPP} ARGS ${CPP_ARGS_LD_SCRIPT} BYPRODUCTS ${LD_SCRIPT_PP})
