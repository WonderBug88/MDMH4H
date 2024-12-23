#include "button.h"
#include "oled.h"

// Global Variables
static int lastButtonState1 = GPIO_PIN_SET;  // Initial state for Button 1
static int lastButtonState2 = GPIO_PIN_SET;  // Initial state for Button 2
static uint32_t pressStartTime1 = 0;         // Start time for Button 1
static uint32_t pressStartTime2 = 0;         // Start time for Button 2
static uint32_t pressDuration1 = 0;          // Press duration for Button 1
static uint32_t pressDuration2 = 0;          // Press duration for Button 2
static ButtonAction lastActionButton1 = IDLE;  // Track last action for Button 1
static ButtonAction lastActionButton2 = IDLE;  // Track last action for Button 2

// Function to initialize both buttons GPIO
void Button_Init(void) {
    __HAL_RCC_GPIOA_CLK_ENABLE();  // Enable GPIOA clock
    
    GPIO_InitTypeDef GPIO_InitStruct = {0};
    
    // Configure Button 1
    GPIO_InitStruct.Pin = BUTTON_PIN_1;    
    GPIO_InitStruct.Mode = GPIO_MODE_INPUT;
    GPIO_InitStruct.Pull = GPIO_PULLUP;
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
    HAL_GPIO_Init(BUTTON_PORT, &GPIO_InitStruct);
    
    // Configure Button 2
    GPIO_InitStruct.Pin = BUTTON_PIN_2;    
    HAL_GPIO_Init(BUTTON_PORT, &GPIO_InitStruct);
}

// Helper function to get press duration
uint32_t Button_GetPressDuration(uint8_t buttonNum) {
    GPIO_PinState buttonState;
    uint32_t *pressStartTime, *pressDuration;
    int *lastButtonState;

    if (buttonNum == 1) {
        buttonState = HAL_GPIO_ReadPin(BUTTON_PORT, BUTTON_PIN_1);
        pressStartTime = &pressStartTime1;
        pressDuration = &pressDuration1;
        lastButtonState = &lastButtonState1;
    } else if (buttonNum == 2) {
        buttonState = HAL_GPIO_ReadPin(BUTTON_PORT, BUTTON_PIN_2);
        pressStartTime = &pressStartTime2;
        pressDuration = &pressDuration2;
        lastButtonState = &lastButtonState2;
    } else {
        return 0;  // Invalid button number
    }

    uint32_t currentTime = HAL_GetTick();

    // Detect press start
    if (buttonState == GPIO_PIN_RESET && *lastButtonState == GPIO_PIN_SET) {
        *pressStartTime = currentTime;
        *pressDuration = 0;  // Reset duration on new press
    }
    // Calculate duration if button is held
    else if (buttonState == GPIO_PIN_RESET && *lastButtonState == GPIO_PIN_RESET) {
        *pressDuration = currentTime - *pressStartTime;
    }
    // Calculate duration if button is released
    else if (buttonState == GPIO_PIN_SET && *lastButtonState == GPIO_PIN_RESET) {
        *pressDuration = currentTime - *pressStartTime;
    }

    *lastButtonState = buttonState;
    return *pressDuration;
}

// Function to handle button actions with debounce, including simultaneous press detection
ButtonAction Button_CheckButtonPress(uint8_t buttonNum) {
    uint32_t pressDuration1 = Button_GetPressDuration(1);
    uint32_t pressDuration2 = Button_GetPressDuration(2);
    ButtonAction action = IDLE;
    
    // Detect simultaneous press for calibration confirmation
    if (HAL_GPIO_ReadPin(BUTTON_PORT, BUTTON_PIN_1) == GPIO_PIN_RESET && 
        HAL_GPIO_ReadPin(BUTTON_PORT, BUTTON_PIN_2) == GPIO_PIN_RESET) {
        
        action = CALIBRATION_CONFIRMATION;  // Unique action for simultaneous press
    }
    else if (buttonNum == 1) {
        if (pressDuration1 > 0) {
            if (pressDuration1 >= LONG_PRESS_MIN && pressDuration1 <= LONG_PRESS_MAX) {
                action = RETURN_TO_MENU;
            } else if (pressDuration1 < SHORT_PRESS_DURATION) {
                action = SCROLL;
            }
        }
    } else if (buttonNum == 2) {
        if (pressDuration2 > 0) {
            if (pressDuration2 >= LONG_PRESS_MIN && pressDuration2 <= LONG_PRESS_MAX) {
                action = CALIBRATION_MODE;
            } else if (pressDuration2 < SHORT_PRESS_DURATION) {
                action = RETURN_TO_MENU;
            }
        }
    }

    // Only return an action if it has changed to avoid bouncing
    if ((buttonNum == 1 && action != lastActionButton1) || 
        (buttonNum == 2 && action != lastActionButton2)) {
        if (buttonNum == 1) lastActionButton1 = action;
        if (buttonNum == 2) lastActionButton2 = action;

        HAL_Delay(DEBOUNCE_DELAY);  // Small debounce delay
        return action;
    }

    return IDLE;
}