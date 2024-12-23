#ifndef __OLED_H_ 
#define __OLED_H_

#include <stdint.h>
#include "py32f0xx_hal.h"

#define GREEN_TEXT      0x00FF00  // RGB color for green
#define RED_TEXT        0xFF0000    // RGB color for red
#define YELLOW_TEXT     0xFFFF00 // RGB color for yellow
#define ORANGE_TEXT     0xFFA500  // RGB value for orange

// Enable/disable the Control OLED pin 
#define OLED_CTRL_PIN           GPIO_PIN_7 
#define OLED_CTRL_PORT          GPIOB 


void OLED_Init(void);
void OLED_Test(void);

void OLED_Enable(uint8_t on);

void OLED_DisplayFlashingText(const char *message);
void OLED_DisplayMessageWithColor(const char *message, uint32_t color);
void OLED_DisplayText(uint8_t x, uint8_t y, char *text);
void OLED_DisplayPotValue(uint32_t val);
void OLED_DisplayMessage(const char *message);

void OLED_SetTextColor(uint32_t color);
void OLED_Clear();
void OLED_ClearRow(uint8_t row);

#endif 