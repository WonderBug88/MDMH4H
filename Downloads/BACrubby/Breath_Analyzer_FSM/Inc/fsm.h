#ifndef __FSM_H_
#define __FSM_H_

#include "button.h"  // Include button.h to access ButtonAction

typedef enum { 
    IDLE_STATE,
		OFF_STATE,   
		STATE_RUNNING,
		STATE_STOP,
		FUNCTION_SELECTION,
    BREATHALYZER_WARMUP,
    BREATHALYZER_READY,
    TAKING_READING,
    CALCULATING_BAC,
    BELOW_0,
    BELOW_04,
    BETWEEN_04_AND_08,
    BETWEEN_08_AND_15,
    BETWEEN_15_AND_20,
    OVER_20,
    ACUPRESSURE,
    LOW_INTENSITY,
    MEDIUM_INTENSITY,
    HIGH_INTENSITY,

    ENTER_CALIBRATION,           // Initiates calibration selection
    CONFIRM_CALIBRATION,         // Yes/No confirmation for calibration
    CALIBRATION_CONFIRM_SEQUENCE, // Short and long press confirmation sequence
    CALIBRATION_PROCESS,         // General state for calibration steps
    PREHEAT,                     // Preheating step
    MEASURE_BASELINE,            // Measuring clean air baseline
    ADJUST_SENSITIVITY,          // Adjusting sensor sensitivity
    COMPLETE_CALIBRATION         // Final state indicating calibration completion
} DeviceState_t;
extern DeviceState_t deviceState; // extern declaration

// Define a variable to track the currently active pattern
typedef enum {
    None,
    PULSING,
    WAVE,
    HEART_BEAT
} PatternType;

extern PatternType activePattern; // Declaration in fsm.h
extern uint32_t startTime;   // Start time for non-blocking delays
extern uint32_t currentTime; // Current time


typedef enum {
    MODE_IDLE = 0,
    MODE_BREATH_ANALYZER = 1,
    MODE_ACUPRESSURE,
    MODE_CALIBRATION
} ButtonMode;

void FSM_Init(void);
void FSM_Run(void);
void fsm_update(void);


#endif // __FSM_H_
