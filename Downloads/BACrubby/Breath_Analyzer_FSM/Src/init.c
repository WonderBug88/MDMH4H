#include "init.h"
#include "power.h"

void Hardware_Init(){
    
    /* Initialize HAL Library */
    /* Initialize peripherals */
    Button_Init();
    Led_Init();
    MQ_Init();
    OLED_Init();
    Motor_TurnOff(); 
    Motor_Init();      // Initialize motor
    PWM_Init();        // Initialize PWM for motor control
}
void APP_ExtiConfig(void)
{
    GPIO_InitTypeDef  GPIO_InitStruct = {0};

    __HAL_RCC_GPIOA_CLK_ENABLE();                  /* Enable GPIOA clock */

    GPIO_InitStruct.Mode  = GPIO_MODE_IT_FALLING;  /* Falling edge interrupt */
    GPIO_InitStruct.Pull  = GPIO_PULLUP;           /* Pull-up resistor */
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_HIGH;
    GPIO_InitStruct.Pin = GPIO_PIN_7;
    HAL_GPIO_Init(GPIOA, &GPIO_InitStruct);

    HAL_NVIC_EnableIRQ(EXTI4_15_IRQn);             /* Enable EXTI interrupt */
    HAL_NVIC_SetPriority(EXTI4_15_IRQn, 0, 0);     /* Set priority */

}