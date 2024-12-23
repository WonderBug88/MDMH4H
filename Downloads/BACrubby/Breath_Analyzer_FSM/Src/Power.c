/* power.c */
#include "power.h"
#include "py32f0xx_ll_rcc.h"
#include "py32f0xx_ll_utils.h"
#include "py32f0xx_ll_system.h"
#include "oled.h"
#include "motor.h"
#include "init.h"


/* Configure the system clock to 24 MHz using HSI */
/* Configure the system clock to 24 MHz */
void APP_SystemClockConfig(void)
{
    /* Enable and calibrate HSI to 24 MHz */
    LL_RCC_HSI_Enable();
    LL_RCC_HSI_SetCalibFreq(LL_RCC_HSICALIBRATION_24MHz);
    while (LL_RCC_HSI_IsReady() != 1) { }

    /* Configure System Clock */
    LL_RCC_SetAHBPrescaler(LL_RCC_SYSCLK_DIV_1);
    LL_RCC_SetSysClkSource(LL_RCC_SYS_CLKSOURCE_HSISYS);
    while (LL_RCC_GetSysClkSource() != LL_RCC_SYS_CLKSOURCE_STATUS_HSISYS) { }

    LL_FLASH_SetLatency(LL_FLASH_LATENCY_0);
    LL_RCC_SetAPB1Prescaler(LL_RCC_APB1_DIV_1);

    /* Configure SysTick */
    HAL_SYSTICK_Config(24000000 / 1000);  // 1 ms tick
    HAL_SYSTICK_CLKSourceConfig(SYSTICK_CLKSOURCE_HCLK);

    /* Update SystemCoreClock */
    SystemCoreClockUpdate();
}


void APP_EnterStop(void)
{
    // Prepare for STOP mode
    OLED_Clear();
    Motor_TurnOff();
    HAL_SuspendTick();

    // Enter STOP mode
    HAL_PWR_EnterSTOPMode(PWR_LOWPOWERREGULATOR_ON, PWR_STOPENTRY_WFI);

    // After wake-up:
    HAL_ResumeTick();
    APP_SystemClockConfig();  
    Motor_Init();             
    OLED_Init();              
    APP_ExtiConfig();   // Move this here for clarity
}

