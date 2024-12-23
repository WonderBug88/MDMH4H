/* 
 * Author: Mahmoud Ahmed 
 * Date: 16 Sep, 2024
 *
 * Target: PY32F002Ax5
 * Project name: Breath Analyzer
 * Description:
 * 
*/

#ifndef __MAIN_H
#define __MAIN_H

#include "py32f0xx_hal.h"


void Screen_showPotValue(uint32_t val);
void Button_waitToStart(void);
void Button_chooseMode(void);
void VibrationToggleOnPress(void);

#endif /* __MAIN_H */

/************************ (C) COPYRIGHT Puya *****END OF FILE******************/
