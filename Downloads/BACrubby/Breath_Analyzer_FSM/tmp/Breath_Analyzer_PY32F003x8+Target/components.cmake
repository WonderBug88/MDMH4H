# components.cmake

# component ARM::CMSIS:CORE@6.1.0
add_library(ARM_CMSIS_CORE_6_1_0 INTERFACE)
target_include_directories(ARM_CMSIS_CORE_6_1_0 INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
  ${CMSIS_PACK_ROOT}/ARM/CMSIS/6.1.0/CMSIS/Core/Include
)
target_compile_definitions(ARM_CMSIS_CORE_6_1_0 INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)

# component Puya::Device:Startup@1.0.0
add_library(Puya_Device_Startup_1_0_0 OBJECT
  "${SOLUTION_ROOT}/RTE/Device/PY32F003x8/startup_py32f003x8.s"
  "${SOLUTION_ROOT}/RTE/Device/PY32F003x8/system_py32f0xx.c"
)
target_include_directories(Puya_Device_Startup_1_0_0 PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
  ${CMSIS_PACK_ROOT}/Puya/PY32F0xx_DFP/1.2.2/Drivers/CMSIS/Device/PY32F0xx/Include
)
target_compile_definitions(Puya_Device_Startup_1_0_0 PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)
target_compile_options(Puya_Device_Startup_1_0_0 PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_OPTIONS>
)
target_link_libraries(Puya_Device_Startup_1_0_0 PUBLIC
  ${CONTEXT}_ABSTRACTIONS
)
set(COMPILE_DEFINITIONS
  _RTE_
)
cbuild_set_defines(AS_ARM COMPILE_DEFINITIONS)
set_source_files_properties("${SOLUTION_ROOT}/RTE/Device/PY32F003x8/startup_py32f003x8.s" PROPERTIES
  COMPILE_FLAGS "${COMPILE_DEFINITIONS}"
)
