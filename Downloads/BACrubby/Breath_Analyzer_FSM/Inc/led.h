#ifndef __LED_H_
#define __LED_H_

#include "py32f0xx_hal.h"

// Button Constants for Readability
#define LED_PIN              GPIO_PIN_5
#define LED_PORT             GPIOB


void Led_Init();
void Led_On();
void Led_Off();
void Led_Toggle();

#endif 