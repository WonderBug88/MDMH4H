/* 
 * Author: Uhrej3
 * Date: 16 Sep, 2024
 *
 * Target: PY32F003x8
 * Project name: Breath Analyzer
 * Description:
 * 
*/

#include "init.h"
#include "fsm.h"
#include "power.h"
void APP_ErrorHandler(void);
// Configure the PA7 as wakeup interrupt

void testADC() {
    OLED_DisplayText(0,0, "Read ADC");
    uint32_t adc = MQ_GetADCValue();

    char data[20];
    snprintf(data, sizeof(data), "ADC= %u", adc);
    OLED_DisplayText(0,20, data);
}

int main(void)
{
    // Initialize hardware and peripherals
    HAL_Init();
		Hardware_Init();
    FSM_Init();
		APP_ExtiConfig();

    // Display initialization message
    OLED_DisplayText(0, 0, "Initializing...");
    HAL_Delay(1000);

   while (1) {
        FSM_Run();

    if (deviceState == OFF_STATE) {
    APP_EnterStop(); // Comment out for now during debugging
    // You could also add something like:
}
    }
}

// Handle error system
void APP_ErrorHandler(void) {
    while (1) {
        Led_Toggle();
        HAL_Delay(1000);
    }
}
