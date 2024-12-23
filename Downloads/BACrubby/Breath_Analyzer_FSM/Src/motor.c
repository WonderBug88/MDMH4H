#include "motor.h"


// Timer handle for TIM1
TIM_HandleTypeDef htim1;
static int pulsingState = 0;
static int waveIntensity = 100;  // Only used for the wave pattern
static uint32_t lastUpdateTime = 0;  // Track last update time


void Motor_Init(void) {
    GPIO_InitTypeDef GPIO_InitStruct = {0};

    // Enable GPIOA clock for motor control on PA1
    __HAL_RCC_GPIOA_CLK_ENABLE();

    GPIO_InitStruct.Pin = GPIO_PIN_1;
    GPIO_InitStruct.Mode = GPIO_MODE_AF_PP;       // Alternate Function Push-Pull
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
    GPIO_InitStruct.Alternate = GPIO_AF13_TIM1;    // Set alternate function for TIM1 on PA1 as TIM1_CH4
    HAL_GPIO_Init(GPIOA, &GPIO_InitStruct);
}

void Motor_TurnOff(void) {
    Motor_SetIntensity(0);  // Set PWM duty cycle to 0% to turn off the motor
}

void PWM_Init(void) {
    // Enable TIM1 clock
    __HAL_RCC_TIM1_CLK_ENABLE();

    // Configure TIM1 for PWM
    TIM_OC_InitTypeDef sConfigOC = {0};
    htim1.Instance = TIM1;
    htim1.Init.Prescaler = 80-1;                 // Adjust prescaler for desired frequency
    htim1.Init.Period = 39;                  // Set period for desired frequency
    htim1.Init.ClockDivision = TIM_CLOCKDIVISION_DIV1;
    htim1.Init.CounterMode = TIM_COUNTERMODE_UP;
    HAL_TIM_PWM_Init(&htim1);

    // Set up TIM1 Channel 4 (assuming PA1 is configured as TIM1_CH4)
    sConfigOC.OCMode = TIM_OCMODE_PWM1;
    sConfigOC.Pulse = htim1.Init.Period; // Set to maximum for 100% duty cycle 
    sConfigOC.OCPolarity = TIM_OCPOLARITY_HIGH;
    sConfigOC.OCFastMode = TIM_OCFAST_DISABLE;
    HAL_TIM_PWM_ConfigChannel(&htim1, &sConfigOC, TIM_CHANNEL_4);

    // Start PWM on TIM1 Channel 4
    HAL_TIM_PWM_Start(&htim1, TIM_CHANNEL_4);
}

// Function to set the PWM duty cycle on TIM1 Channel 4
void Motor_SetIntensity(uint8_t dutyCycle) {
    if (dutyCycle > 100) dutyCycle = 100;  // Clamp to 100%
    uint32_t pulse = (htim1.Init.Period + 1) * dutyCycle / 100;
    __HAL_TIM_SET_COMPARE(&htim1, TIM_CHANNEL_4, pulse);
}


// Control motor based on predefined intensity levels
void Motor_Control(MotorIntensity_t intensity) {
    switch (intensity) {
        case LowIntensity:
            Motor_SetIntensity(50); // Low intensity, 20% duty cycle
            break;
        case MediumIntensity:
            Motor_SetIntensity(80); // Medium intensity, 50% duty cycle
            break;
        case HighIntensity:
            Motor_SetIntensity(100); // High intensity, 80% duty cycle
            break;
        case Pulsing:
            Motor_Pulsing();
            break;
        case HeartBeat:
            Motor_HeartBeat();
            break;
        case Wave:
            Motor_Wave();
            break;
        default:
            Motor_TurnOff();
    }
}

/* Basic Vibration Patterns */
void Motor_VibrateShort(void) {
    Motor_SetIntensity(30);  // Short pulse at 30% duty cycle
    HAL_Delay(100);
    Motor_TurnOff();
}

void Motor_VibrateDoubleShort(void) {
    Motor_VibrateShort();
    HAL_Delay(100);
    Motor_VibrateShort();
}

void Motor_VibrateLong(void) {
    Motor_SetIntensity(100);  // Long pulse at 100% duty cycle
    HAL_Delay(500);
    Motor_TurnOff();
}

void Motor_VibrateTriple(void) {
    Motor_VibrateShort();
    HAL_Delay(100);
    Motor_VibrateShort();
    HAL_Delay(100);
    Motor_VibrateShort();
}

void Motor_VibrateDoubleLong(void) {
    Motor_VibrateLong();
    HAL_Delay(200);
    Motor_VibrateLong();
}

void Motor_VibrateContinuous(void) {
    Motor_SetIntensity(100);  // Continuous at 100% duty cycle
}

/* Advanced Patterns */

// Pulsiing pattern: ramp up and down in intensity
void Motor_Pulsing(void) {
    if (pulsingState <= 10) {
        Motor_SetIntensity(pulsingState * 10); // Ramp up
        HAL_Delay(100);                        // Speed of ramp-up
        pulsingState++;
    } else if (pulsingState <= 20) {
        Motor_SetIntensity((20 - pulsingState) * 10); // Ramp down
        HAL_Delay(100);                        // Speed of ramp-down
        pulsingState++;
    } else {
        pulsingState = 0; // Reset to loop again
    }
}

// Heartbeat pattern: quick double pulse
void Motor_HeartBeat(void) {
    switch (pulsingState) {
        case 0:
            Motor_SetIntensity(100); // First strong pulse
            HAL_Delay(500);          // Duration for the first pulse
            Motor_TurnOff();
            pulsingState = 1;        // Move to the next state
            break;
        case 1:
            HAL_Delay(50);           // Short pause between pulses
            Motor_SetIntensity(80);  // Second, slightly weaker pulse
            HAL_Delay(1000);         // Duration for the second pulse
            Motor_TurnOff();
            pulsingState = 2;        // Move to the next state
            break;
        case 2:
            HAL_Delay(50);           // Longer pause before next heartbeat
            pulsingState = 0;        // Reset to loop again
            break;
    }
}

// Wave pattern: gradually increase and decrease intensity
void Motor_Wave(void) {
    uint32_t currentTime = HAL_GetTick();  // Get current system time in ms

    // Only update the wave pattern every 100ms
    if ((currentTime - lastUpdateTime) < 100) {
        return;  // Exit function if 100ms has not passed
    }
    lastUpdateTime = currentTime;  // Reset the last update time

    // Wave pattern logic
    if (pulsingState == 0) {
        Motor_SetIntensity(waveIntensity);  // Set current intensity
        waveIntensity += 10;  // Increase intensity

        if (waveIntensity >= 100) {
            waveIntensity = 100;  // Cap at maximum intensity
            pulsingState = 1;     // Move to decreasing phase
        }
    } else if (pulsingState == 1) {
        Motor_SetIntensity(waveIntensity);  // Set current intensity
        waveIntensity -= 10;  // Decrease intensity

        if (waveIntensity <= 10) {
            waveIntensity = 10;  // Cap at minimum intensity
            pulsingState = 0;    // Reset to start increasing again
        }
    }
}
