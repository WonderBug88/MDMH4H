#ifndef __MOTOR_H_
#define __MOTOR_H_

#include "py32f0xx_hal.h"

// Define motor pin and port
#define MOTOR_PIN               GPIO_PIN_1
#define MOTOR_PORT              GPIOA

// Define motor intensities using an enum
typedef enum {
    LowIntensity = 1,
    MediumIntensity,
    HighIntensity,
    Pulsing,
    HeartBeat,
    Wave
} MotorIntensity_t;

// Function prototypes
void Motor_Init(void);                           // Initialize motor GPIO and PWM
void PWM_Init(void);                             // Initialize Timer for PWM control on MOTOR_PIN
void Motor_Control(MotorIntensity_t intensity);  // Control motor based on intensity mode
void Motor_SetIntensity(uint8_t dutyCycle);      // Set motor PWM duty cycle (0-100%)


// Basic Vibration Patterns
void Motor_TurnOff(void);                        // Turn motor off
void Motor_VibrateShort(void);                   // Short pulse vibration
void Motor_VibrateDoubleShort(void);             // Double short pulse
void Motor_VibrateLong(void);                    // Long pulse vibration
void Motor_VibrateTriple(void);                  // Triple short pulse
void Motor_VibrateDoubleLong(void);              // Double long pulse
void Motor_VibrateContinuous(void);              // Continuous vibration

// Intensity-based functions
void Motor_Low(void);                            // Low intensity vibration
void Motor_Medium(void);                         // Medium intensity vibration
void Motor_High(void);                           // High intensity vibration

// Advanced Patterns
void Motor_Pulsing(void);                        // Pulsing vibration (e.g., ramp up and down)
void Motor_HeartBeat(void);                      // Heartbeat vibration pattern
void Motor_Wave(void);                           // Wave pattern with gradual increase and decrease in intensity



#endif // __MOTOR_H_
