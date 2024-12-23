# groups.cmake

# group Src
add_library(Group_Src OBJECT
  "${SOLUTION_ROOT}/Src/fonts.c"
  "${SOLUTION_ROOT}/Src/main.c"
  "${SOLUTION_ROOT}/Src/init.c"
  "${SOLUTION_ROOT}/Src/py32f0xx_hal_msp.c"
  "${SOLUTION_ROOT}/Src/py32f0xx_it.c"
  "${SOLUTION_ROOT}/Src/ssd1306.c"
  "${SOLUTION_ROOT}/Src/oled.c"
  "${SOLUTION_ROOT}/Src/button.c"
  "${SOLUTION_ROOT}/Src/led.c"
  "${SOLUTION_ROOT}/Src/mq303b.c"
  "${SOLUTION_ROOT}/Src/fsm.c"
  "${SOLUTION_ROOT}/Src/motor.c"
)
target_include_directories(Group_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
)
target_compile_definitions(Group_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)
target_compile_options(Group_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_OPTIONS>
)
target_link_libraries(Group_Src PUBLIC
  ${CONTEXT}_ABSTRACTIONS
)

# group HAL_Drivers/Inc
add_library(Group_HAL_Drivers_Inc INTERFACE)
target_include_directories(Group_HAL_Drivers_Inc INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
  ${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Inc
)
target_compile_definitions(Group_HAL_Drivers_Inc INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)

# group HAL_Drivers/Src
add_library(Group_HAL_Drivers_Src OBJECT
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_adc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_adc_ex.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_comp.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_cortex.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_crc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_exti.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_gpio.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_i2c.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_led.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_pwr.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_rcc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_tim.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_uart.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_adc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_comp.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_crc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_dma.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_exti.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_gpio.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_i2c.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_led.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_lptim.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_pwr.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_rcc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_rtc.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_spi.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_ll_tim.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_dma.c"
  "${SOLUTION_ROOT}/Drivers/PY32F0xx_HAL_Driver/Src/py32f0xx_hal_tim_ex.c"
)
target_include_directories(Group_HAL_Drivers_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
)
target_compile_definitions(Group_HAL_Drivers_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)
target_compile_options(Group_HAL_Drivers_Src PUBLIC
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_OPTIONS>
)
target_link_libraries(Group_HAL_Drivers_Src PUBLIC
  ${CONTEXT}_ABSTRACTIONS
)

# group Inc
add_library(Group_Inc INTERFACE)
target_include_directories(Group_Inc INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_INCLUDE_DIRECTORIES>
  ${SOLUTION_ROOT}/Inc
)
target_compile_definitions(Group_Inc INTERFACE
  $<TARGET_PROPERTY:${CONTEXT},INTERFACE_COMPILE_DEFINITIONS>
)
