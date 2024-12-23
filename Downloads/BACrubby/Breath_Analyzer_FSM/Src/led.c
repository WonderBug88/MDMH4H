#include "led.h"

void Led_Init()
{
	/* RCC Clock Enable */
	__HAL_RCC_GPIOB_CLK_ENABLE();
	
	/* LED (PA1) Initialize */
	GPIO_InitTypeDef led = {0};
	led.Mode = GPIO_MODE_OUTPUT_PP;
	led.Pin = LED_PIN;
	led.Pull = GPIO_NOPULL;
	led.Speed = GPIO_SPEED_FREQ_LOW;
	led.Alternate = 0;	
	HAL_GPIO_Init(LED_PORT, &led);
}
void Led_On()
{
    HAL_GPIO_WritePin(LED_PORT, LED_PIN, GPIO_PIN_SET);
}
void Led_Off()
{
    HAL_GPIO_WritePin(LED_PORT, LED_PIN, GPIO_PIN_RESET);
}
void Led_Toggle()
{
     HAL_GPIO_TogglePin(LED_PORT, LED_PIN);
}