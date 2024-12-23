#ifndef __INIT_H_
#define __INIT_H_

#include "fsm.h"
#include "oled.h"
#include "led.h"
#include "ssd1306.h"
#include "mq303b.h"
#include "button.h"
#include "motor.h"

void Hardware_Init();
void APP_ExtiConfig();

#endif // __INIT_H_