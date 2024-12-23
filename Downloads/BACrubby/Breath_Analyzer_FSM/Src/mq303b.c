#include "mq303b.h"
#include "py32f0xx_hal.h"

// Global variables for ADC and PWM
ADC_HandleTypeDef AdcHandle;
TIM_HandleTypeDef htim3;  // Use TIM3 for MQ303B Heater PWM
TIM_HandleTypeDef htim16;  // Use TIM16 for MQ303B VC control PWM
uint32_t g_adcValue = 0;  // Global variable for ADC value

// Utility function for PWM initialization
void MQ_PWM_Init(TIM_HandleTypeDef *htim, GPIO_TypeDef *GPIOx, uint16_t GPIO_Pin, 
                 uint32_t AlternateFunction, uint32_t TimerChannel, uint32_t Prescaler, 
                 uint32_t Period) {
    // Enable GPIO clock
    __HAL_RCC_GPIOA_CLK_ENABLE();

    GPIO_InitTypeDef GPIO_InitStruct = {0};
    GPIO_InitStruct.Pin = GPIO_Pin;
    GPIO_InitStruct.Mode = GPIO_MODE_AF_PP;
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
    GPIO_InitStruct.Alternate = AlternateFunction;
    HAL_GPIO_Init(GPIOx, &GPIO_InitStruct);

    // Timer configuration
    TIM_OC_InitTypeDef sConfigOC = {0};
    htim->Init.Prescaler = Prescaler - 1;
    htim->Init.Period = Period - 1;
    htim->Init.ClockDivision = TIM_CLOCKDIVISION_DIV1;
    htim->Init.CounterMode = TIM_COUNTERMODE_UP;

    HAL_TIM_PWM_Init(htim);

    sConfigOC.OCMode = TIM_OCMODE_PWM1;
    sConfigOC.Pulse = 0;  // Start with 0% duty cycle
    sConfigOC.OCPolarity = TIM_OCPOLARITY_HIGH;
    sConfigOC.OCFastMode = TIM_OCFAST_DISABLE;

    HAL_TIM_PWM_ConfigChannel(htim, &sConfigOC, TimerChannel);
    HAL_TIM_PWM_Start(htim, TimerChannel);
}

// Function to initialize the MQ303B heater (PA4, TIM3)
void MQ_HeaterInit(void) {
    __HAL_RCC_TIM3_CLK_ENABLE();  // Enable TIM3 clock
    htim3.Instance = TIM3;

    MQ_PWM_Init(&htim3, GPIOA, GPIO_PIN_4, GPIO_AF13_TIM3, TIM_CHANNEL_3, 8, 100);
}

// Function to initialize the MQ303B VC control (PA6, TIM16)
void MQ_VCInit(void) {
    __HAL_RCC_TIM16_CLK_ENABLE();  // Enable TIM16 clock
    htim16.Instance = TIM16;

    MQ_PWM_Init(&htim16, GPIOA, GPIO_PIN_6, GPIO_AF5_TIM16, TIM_CHANNEL_1, 8, 100);
}

// Function to initialize ADC for MQ303B (PA5)
void MQ_ADCInit(void) {
    __HAL_RCC_ADC_CLK_ENABLE();

    AdcHandle.Instance = ADC1;
    AdcHandle.Init.ClockPrescaler = ADC_CLOCK_SYNC_PCLK_DIV1;
    AdcHandle.Init.Resolution = ADC_RESOLUTION_12B;
    AdcHandle.Init.DataAlign = ADC_DATAALIGN_RIGHT;
    AdcHandle.Init.ScanConvMode = DISABLE;
    AdcHandle.Init.EOCSelection = ADC_EOC_SINGLE_CONV;
    AdcHandle.Init.LowPowerAutoWait = DISABLE;
    AdcHandle.Init.ContinuousConvMode = DISABLE;
    AdcHandle.Init.DiscontinuousConvMode = DISABLE;
    AdcHandle.Init.ExternalTrigConv = ADC_SOFTWARE_START;
    AdcHandle.Init.ExternalTrigConvEdge = ADC_EXTERNALTRIGCONVEDGE_NONE;
    AdcHandle.Init.DMAContinuousRequests = DISABLE;
    AdcHandle.Init.Overrun = ADC_OVR_DATA_OVERWRITTEN;
    HAL_ADC_Init(&AdcHandle);

    ADC_ChannelConfTypeDef sConfig = {0};
    sConfig.Channel = ADC_CHANNEL_5;  // PA5
    sConfig.Rank = 1;
    sConfig.SamplingTime = ADC_SAMPLETIME_239CYCLES_5;
    HAL_ADC_ConfigChannel(&AdcHandle, &sConfig);
}

// Main MQ303B initialization function
void MQ_Init(void) {
    MQ_HeaterInit();  // Initialize heater
    MQ_VCInit();      // Initialize VC control
    MQ_ADCInit();     // Initialize ADC
}

// Retrieve the latest ADC value
uint32_t MQ_GetADCValue(void) {
    HAL_ADC_Start(&AdcHandle);
    HAL_ADC_PollForConversion(&AdcHandle, HAL_MAX_DELAY);
    return HAL_ADC_GetValue(&AdcHandle);
}

// Set Heater Voltage
void MQ303B_SetHeaterVoltage(float voltage) {
    const float maxVoltage = 3.3f;
    if (voltage > maxVoltage) voltage = maxVoltage;
    if (voltage < 0.0f) voltage = 0.0f;

    float dutyCycle = (voltage / maxVoltage) * 100.0f;
    uint32_t pulse = (uint32_t)((htim3.Init.Period + 1) * dutyCycle / 100.0f);
    __HAL_TIM_SET_COMPARE(&htim3, TIM_CHANNEL_3, pulse);
}

// Set VC Voltage
void MQ303B_SetVCVoltage(float voltage) {
    const float maxVoltage = 3.3f;
    if (voltage > maxVoltage) voltage = maxVoltage;
    if (voltage < 0.0f) voltage = 0.0f;

    float dutyCycle = (voltage / maxVoltage) * 100.0f;
    uint32_t pulse = (uint32_t)((htim16.Init.Period + 1) * dutyCycle / 100.0f);
    __HAL_TIM_SET_COMPARE(&htim16, TIM_CHANNEL_1, pulse);
}

// Warmup Mode (2.2V for heater)
void MQ303B_WarmupMode(float voltage) {
    HAL_Delay(15000);                   // Hold for warmup duration
    MQ303B_SetHeaterVoltage(voltage);  // Set heater to specified voltage
}

// Operating Mode (3.0 Set test voltage 3.0V)
float MQ303B_OperatingMode(void) {
    uint32_t highestAdcValue = 0;  // Variable to store the highest ADC value
    uint32_t startTime = HAL_GetTick();  // Record the start time of the blowing period

    // Set the heater to 3.0V for operating mode
    MQ303B_SetHeaterVoltage(3.0f);

    // Start ADC sampling for the blowing period (10 seconds)
    while ((HAL_GetTick() - startTime) < 10000) {
        HAL_ADC_Start(&AdcHandle);  // Start ADC conversion
        if (HAL_ADC_PollForConversion(&AdcHandle, 10) == HAL_OK) {
            uint32_t adcValue = HAL_ADC_GetValue(&AdcHandle);  // Get ADC value

            // Update the highest reading
            if (adcValue > highestAdcValue) {
                highestAdcValue = adcValue;
            }
        }
    }

    // Turn off the heater after the blowing period
    MQ303B_SetHeaterVoltage(0.0f);

    // Convert the highest ADC value to BAC
    float bac = MQ_ConvertBAC(highestAdcValue);

    // Return the calculated BAC for display in FSM
    return bac;
}

// Convert ADC reading to BAC
float MQ_ConvertBAC(uint32_t adcValue) {
    float voltage = (adcValue * 3.3f) / 4096;

    // Convert voltage to PPM based on sensitivity curve
    float ppm;
    if (voltage <= 1.5f) {
        ppm = (voltage - 1.0f) * 100.0f / 0.5f;  // Linear between 0-100 ppm
    } else if (voltage <= 2.5f) {
        ppm = 100.0f + (voltage - 1.5f) * 400.0f / 1.0f;  // Linear between 100-500 ppm
    } else {
        ppm = 500.0f;  // Cap ppm at 500
    }

    // Convert PPM to BAC using a 2100:1 ratio
    return ppm / 2100.0f;
}
