#include "fsm.h"
#include "button.h"
#include "oled.h"
#include "motor.h"
#include "power.h"
#include "mq303b.h"
#include "led.h"
#include <string.h>

#define OFFSET_ROW 20

PatternType activePattern = None; // Define and initialize activePattern
DeviceState_t deviceState = OFF_STATE; // Definition (no static keyword)
ButtonMode buttonMode = MODE_IDLE;
float alcoholLevel = 0.12f;
uint32_t lastAdcValue = 0; // Store the last ADC value read for BAC calculation


void FSM_Init() {
    deviceState = OFF_STATE; // Ensure the device starts in OFF_STATE
    alcoholLevel = 0;
}

void FSM_Run() {
    static DeviceState_t lastDeviceState = IDLE_STATE; // Not OFF_STATE
    static int selectedMode = 0;
    static int lastSelectedMode = -1;
    static uint32_t startTime = 0;   // Static local variable for non-blocking timing
    uint32_t currentTime = HAL_GetTick();  // Get current time at each call to FSM_Run
    ButtonAction actionButton1 = Button_CheckButtonPress(1);   // Check Button 1 action for scrolling
    ButtonAction actionButton2 = Button_CheckButtonPress(2);   // Check Button 2 action for selection/back

    // Check if device state has changed
    if (deviceState != lastDeviceState || selectedMode != lastSelectedMode) {
        OLED_Clear();

        // If we just transitioned into OFF_STATE, enter STOP mode once
        if (deviceState == OFF_STATE && lastDeviceState != OFF_STATE) {
            // Display going to sleep message
            OLED_DisplayText(0, 0, "OFF...");
            HAL_Delay(1000);

 
        }

        else if (deviceState == FUNCTION_SELECTION) {
            OLED_DisplayText(0, 0, (selectedMode == 0) ? "> 1: Breathalyzer" : "  1: Breathalyzer");
            OLED_DisplayText(0, 20, (selectedMode == 1) ? "> 2: Acupressure" : "  2: Acupressure");
            OLED_DisplayText(0, 40, (selectedMode == 2) ? "> 3: Calibration" : "  3: Calibration");
        }

        // ACUPRESSURE: Submenu for selecting acupressure intensity modes
    else if (deviceState == ACUPRESSURE) {
            OLED_DisplayText(0, 0, "Acupressure Modes:");

            // Display two options at a time
            int maxVisibleOptions = 2;
            int startIndex = (selectedMode / maxVisibleOptions) * maxVisibleOptions;

            if (startIndex == 0) {
                OLED_DisplayText(0, OFFSET_ROW, (selectedMode == 0) ? "> Low Intensity" : "  Low Intensity");
                OLED_DisplayText(0, 2 * OFFSET_ROW, (selectedMode == 1) ? "> Medium Intensity" : "  Medium Intensity");
            } else if (startIndex == 2) {
                OLED_DisplayText(0, OFFSET_ROW, (selectedMode == 2) ? "> High Intensity" : "  High Intensity");
                OLED_DisplayText(0, 2 * OFFSET_ROW, (selectedMode == 3) ? "> Heartbeat" : "  Heartbeat");
            } else if (startIndex == 4) {
                OLED_DisplayText(0, OFFSET_ROW, (selectedMode == 4) ? "> Pulsing" : "  Pulsing");
                OLED_DisplayText(0, 2 * OFFSET_ROW, (selectedMode == 5) ? "> Wave" : "  Wave");
            }
        }
				// CONFIRM_CALIBRATION: Enter Calibration Mode
		else if (deviceState == CONFIRM_CALIBRATION) {
				OLED_DisplayText(0, 0, "Confirm Calibration?");
				OLED_DisplayText(0, OFFSET_ROW, "Press Both Buttons");
    
				if (actionButton1 == CALIBRATION_CONFIRMATION) {
						deviceState = CALIBRATION_PROCESS;
				}
		} else if (deviceState == CALIBRATION_PROCESS) {
				static uint32_t calibrationStartTime = 0;

				if (calibrationStartTime == 0) {
						calibrationStartTime = HAL_GetTick(); // Record start time
						MQ303B_SetHeaterVoltage(2.2f); // Start calibration
				}

				OLED_DisplayText(0, 0, "Calibration Process:");
				if (HAL_GetTick() - calibrationStartTime < 60000) {
						OLED_DisplayText(0, OFFSET_ROW, "Preheating...");
				} else if (HAL_GetTick() - calibrationStartTime < 120000) {
						OLED_DisplayText(0, OFFSET_ROW, "Measuring Baseline...");
				} else if (HAL_GetTick() - calibrationStartTime < 180000) {
						OLED_DisplayText(0, OFFSET_ROW, "Adjusting Sensitivity...");
				} else {
						OLED_DisplayText(0, OFFSET_ROW, "Calibration Complete!");
						MQ303B_SetHeaterVoltage(0); // Turn off heater
						calibrationStartTime = 0; // Reset timer
						deviceState = FUNCTION_SELECTION; // Return to main menu
				}
		}

			// BREATHALYZER_WARMUP State
			else if (deviceState == BREATHALYZER_WARMUP) {
					OLED_DisplayText(0, 0, "Warming up...");
					MQ303B_SetHeaterVoltage(2.2f);  // Initial heater warmup at 2.2V
					startTime = currentTime;        // Record the start time
					// Wait for 15 seconds in this state before transitioning
			}

			// TAKING_READING State: Instruct user to blow
			else if (deviceState == TAKING_READING) {
					OLED_DisplayText(0, 0, "Blow into cap");
					OLED_DisplayText(0, OFFSET_ROW, "for 5 seconds");
					MQ303B_SetHeaterVoltage(3.0f);    // Set test voltage to 3.0V for accurate reading
					startTime = currentTime;
    // Keep ADC active during this period to capture the reading
}

		// CALCULATING_BAC State: Display BAC Calculation Message
			else if (deviceState == CALCULATING_BAC) {
					MQ303B_SetHeaterVoltage(0.0f);  // Turn off heater after measurement
					OLED_DisplayText(0, 0, "Calculating BAC...");
}
        // BAC RESULT STATES: Display results based on BAC level and vibrate feedback
        else if (deviceState == BELOW_0) {
            OLED_DisplayText(0, 0, "BAC 0.00");
            OLED_DisplayText(0, OFFSET_ROW, "Grab your keys, ready to go.");
        } else if (deviceState == BELOW_04) {
            OLED_DisplayText(0, 0, "BAC 0.03");
            OLED_DisplayText(0, OFFSET_ROW, "Safe and sound. In control.");
            Motor_VibrateDoubleShort();
        } else if (deviceState == BETWEEN_04_AND_08) {
            OLED_DisplayText(0, 0, "BAC 0.06");
            OLED_DisplayText(0, OFFSET_ROW, "Walking a fine line. Risky.");
            Motor_VibrateLong();
        } else if (deviceState == BETWEEN_08_AND_15) {
            OLED_DisplayText(0, 0, "BAC 0.12");
            OLED_DisplayText(0, OFFSET_ROW, "Time to stop. Pushing it.");
            Motor_VibrateTriple();
        } else if (deviceState == BETWEEN_15_AND_20) {
            OLED_DisplayText(0, 0, "BAC 0.18");
            OLED_DisplayText(0, OFFSET_ROW, "You've gone too far.");
            Motor_VibrateDoubleLong();
        } else if (deviceState == OVER_20) {
            OLED_DisplayText(0, 0, "BAC 0.22");
            OLED_DisplayText(0, OFFSET_ROW, "No driving tonight.");
            Motor_VibrateContinuous();
        }

        lastDeviceState = deviceState;
        lastSelectedMode = selectedMode;
    }
    // State transitions based on elapsed time
if (deviceState == BREATHALYZER_WARMUP && (currentTime - startTime >= 5000)) {
		MQ303B_SetHeaterVoltage(3.0f);  // Adjust heater voltage for sensing
    deviceState = TAKING_READING;
    startTime = currentTime;
} else if (deviceState == TAKING_READING && (currentTime - startTime >= 10000)) {
		MQ303B_SetHeaterVoltage(0.0f);  // Turn off heater after reading
    deviceState = CALCULATING_BAC;
		startTime = currentTime;
} else if (deviceState == CALCULATING_BAC && (currentTime - startTime >= 2000)) {
    float bac = MQ_ConvertBAC(lastAdcValue);  // Calculate BAC from ADC value

    char bacText[20];
    snprintf(bacText, sizeof(bacText), "BAC %.2f", bac);
    OLED_DisplayText(0, 0, bacText);  // Display BAC on OLED

    // Set the device state based on BAC level
    if (bac < 0.04) {
        deviceState = BELOW_0;
    } else if (bac < 0.08) {
        deviceState = BELOW_04;
    } else if (bac < 0.15) {
        deviceState = BETWEEN_04_AND_08;
    } else if (bac < 0.20) {
        deviceState = BETWEEN_15_AND_20;
    } else {
        deviceState = OVER_20;
    }
}
    // Handle button actions for each state
    if (deviceState == OFF_STATE && actionButton1 == RETURN_TO_MENU) {
	// Wake up and go to FUNCTION_SELECTION
        deviceState = FUNCTION_SELECTION;
        OLED_Clear();
    } else if (deviceState == FUNCTION_SELECTION) {
        if (actionButton1 == SCROLL) {
            selectedMode = (selectedMode + 1) % 3;
        } else if (actionButton2 == RETURN_TO_MENU) {
            if (selectedMode == 0) {
                deviceState = BREATHALYZER_WARMUP;
            } else if (selectedMode == 1) {
                deviceState = ACUPRESSURE;
                selectedMode = 0;
            } else if (selectedMode == 2) {
                deviceState = CONFIRM_CALIBRATION;
            }
            OLED_Clear();
        }
        if (actionButton2 == LONG_PRESS) {
            deviceState = OFF_STATE;
            Motor_TurnOff();
            OLED_Clear();
        }
    } else if (deviceState == ACUPRESSURE) {
        if (actionButton1 == SCROLL) {
            selectedMode = (selectedMode + 1) % 6;
        } else if (actionButton2 == RETURN_TO_MENU) {
            switch (selectedMode) {
                case 0:
                    Motor_SetIntensity(50); // Low
                    activePattern = None;
                    break;
                case 1:
                    Motor_SetIntensity(80); // Medium
                    activePattern = None;
                    break;
                case 2:
                    Motor_SetIntensity(100); // High
                    activePattern = None;
                    break;
                case 3:
                    activePattern = HEART_BEAT;
                    break;
                case 4:
                    activePattern = PULSING;
                    break;
                case 5:
                    activePattern = WAVE;
                    break;
            }
            OLED_Clear();
        }
        if (actionButton2 == LONG_PRESS) {
            deviceState = FUNCTION_SELECTION;
            Motor_TurnOff();
            activePattern = None;  // Reset the pattern state
            OLED_Clear();
        }
    }

    // Execute the active pattern in non-blocking mode
    if (deviceState == ACUPRESSURE && activePattern != None) {
        switch (activePattern) {
            case HEART_BEAT:
                Motor_HeartBeat();  // Will run one iteration and return
                break;
            case PULSING:
                Motor_Pulsing();    // Will run one iteration and return
                break;
            case WAVE:
                Motor_Wave();       // Will run one iteration and return
                break;
            default:
                break;
        }
    } else if (deviceState == CONFIRM_CALIBRATION) {
        if (actionButton1 == CALIBRATION_CONFIRMATION && actionButton2 == CALIBRATION_CONFIRMATION) {
            deviceState = CALIBRATION_PROCESS;
            selectedMode = 0;
            OLED_Clear();
					}
				}
    HAL_Delay(150);
	}