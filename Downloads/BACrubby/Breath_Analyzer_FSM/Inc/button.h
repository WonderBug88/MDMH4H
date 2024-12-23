#ifndef __BUTTON_H_
#define __BUTTON_H_

#include <stdint.h>
#include "py32f0xx_hal.h"

// Button pin definitions
#define BUTTON_PIN_1              GPIO_PIN_7  // PA7 for Button 1
#define BUTTON_PIN_2              GPIO_PIN_2  // PA2 for Button 2 (adjust if needed)
#define BUTTON_PORT               GPIOA

// Timing thresholds for button press durations
#define SHORT_PRESS_DURATION       500   // 500ms for a short press
#define LONG_PRESS_DURATION        5000  // 5s for a long press

// Multi-press duration definitions (debounce settings)
#define SHORT_PRESS_MIN            6     // Minimum duration for short press
#define SHORT_PRESS_MAX            500   // Maximum duration for short press
#define LONG_PRESS_MIN             1000  // Minimum duration for long press
#define LONG_PRESS_MAX             3000  // Maximum duration for long press

// Debounce delay in milliseconds
#define DEBOUNCE_DELAY             50    // 50ms debounce delay

typedef enum {
    BUTTON_IDLE,
    BUTTON_PRESSED,
    BUTTON_RELEASED
} ButtonState;

typedef enum {
    IDLE,
    SCROLL,
    ACUPRESSURE_MODE,
    CALIBRATION_MODE,
		CALIBRATION_CONFIRMATION,
    RETURN_TO_MENU,
		LONG_PRESS
} ButtonAction;

// Function Prototypes
void Button_Init(void);
uint8_t Button1_IsPressed(void);
uint8_t Button2_IsPressed(void);
uint32_t Button_GetPressDuration(uint8_t buttonNum);
ButtonAction Button_CheckButtonPress(uint8_t buttonNum);

#endif // __BUTTON_H_
