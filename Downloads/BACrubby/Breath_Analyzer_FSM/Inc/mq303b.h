#ifndef __MQ303B_H_
#define __MQ303B_H_

#include <stdint.h>
#include "py32f0xx_hal.h"


#define HEATER_GPIO_Port    GPIOA           // GPIO Port for the heater
#define HEATER_Pin          GPIO_PIN_4      // Heater control on PA0
#define SENSOR_ADC_CHANNEL  ADC_CHANNEL_5   // ADC Channel for sensor on PA5

#define BLOW_THRESHOLD      500


void MQ303B_PWM_Init(void);
void MQ_Init(void);
void MQ_StartBreathalyzer(void);
uint8_t MQ_IsBlowing(void);

// Get adc value and utilities
float MQ_ConvertBAC(uint32_t adc);
uint16_t MQ_GetADC(void);
uint32_t MQ_GetADCValue(void);

// Calibration function
uint32_t MQ_MeasureBaseline(void);

// Functions for the MQ303B
void Mq303_CalibrationMode(void);                // Set PWM duty cycle to approximately 0.9V for MQ303B calibration  
void MQ303B_WarmupMode(float voltage);	
void MQ303B_SetHeaterVoltage(float voltage);			// Brief warmup at 2.2V
float MQ303B_OperatingMode(void);                 // Set the Sensor to Operating Mode at 0.9V for blowing into cap
void MQ303B_VC_PWM_Init(void);

#endif