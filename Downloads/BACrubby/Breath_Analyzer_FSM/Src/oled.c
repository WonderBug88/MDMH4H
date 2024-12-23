#include "ssd1306.h"
#include "oled.h"

#define I2C_ADDRESS        0xA0     /* host address */
#define I2C_STATE_READY    0
#define I2C_STATE_BUSY_TX  1
#define I2C_STATE_BUSY_RX  2

I2C_HandleTypeDef I2cHandle;

extern void APP_ErrorHandler(void);

void OLED_Init(void)
{
    //Init Control pin 
    GPIO_InitTypeDef GPIO_InitStruct;

    __HAL_RCC_GPIOB_CLK_ENABLE();
    GPIO_InitStruct.Pin = OLED_CTRL_PIN;
    GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
    GPIO_InitStruct.Pull = GPIO_PULLUP;
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_HIGH;
    HAL_GPIO_Init(OLED_CTRL_PORT, &GPIO_InitStruct);

    // Initialize I2C port
    I2cHandle.Instance             = I2C;
    I2cHandle.Init.ClockSpeed      = 100000;        // 100KHz ~ 400KHz
    I2cHandle.Init.DutyCycle       = I2C_DUTYCYCLE_16_9;
    I2cHandle.Init.OwnAddress1     = I2C_ADDRESS;
    I2cHandle.Init.GeneralCallMode = I2C_GENERALCALL_DISABLE;
    I2cHandle.Init.NoStretchMode   = I2C_NOSTRETCH_DISABLE;

    if (HAL_I2C_Init(&I2cHandle) != HAL_OK)
    {
        APP_ErrorHandler();
    }

    OLED_Enable(1);
    SSD1306_Init();
}

void OLED_Enable(uint8_t on)
{
    if (on == 0) {
        HAL_GPIO_WritePin(OLED_CTRL_PORT, OLED_CTRL_PIN, GPIO_PIN_RESET); // Disable OLED
    } else {
        HAL_GPIO_WritePin(OLED_CTRL_PORT, OLED_CTRL_PIN, GPIO_PIN_SET);   // Enable OLED
    }
}



void OLED_Test(void){

    int y1, y2;
    SSD1306_DrawLine(0,   0, 127,  0, 1);
    SSD1306_DrawLine(0,   0,   0, 63, 1);
    SSD1306_DrawLine(127, 0, 127, 63, 1);
    SSD1306_DrawLine(0,  63, 127, 63, 1);
    SSD1306_GotoXY(5, 5);
    SSD1306_Puts("Breathalyzer Device", &Font_11x18, 1);
    SSD1306_GotoXY(10, 52);
    SSD1306_Puts("Font size: 11x18", &Font_6x10, 1);
    SSD1306_UpdateScreen(); // display
    HAL_Delay(1000);

    SSD1306_Fill(0);
    SSD1306_GotoXY(5, 5);
    SSD1306_Puts("Breathalyzer Device", &Font_11x18, 1);
    SSD1306_GotoXY(10, 52);
    SSD1306_Puts("Initialize peripheral ...", &Font_6x12, 1);
    SSD1306_UpdateScreen();
    HAL_Delay(1000);

    SSD1306_ToggleInvert(); // Invert display
    SSD1306_UpdateScreen();
    HAL_Delay(1000);

    SSD1306_ToggleInvert(); // Invert display
    SSD1306_UpdateScreen();
    HAL_Delay(1000);

    SSD1306_Fill(0);
    y1 = 64, y2 = 0;
    while (y1 > 0)
    {
        SSD1306_DrawLine(0, y1, 127, y2, 1);
        SSD1306_UpdateScreen();
        y1 -= 2;
        y2 += 2;
    }
    HAL_Delay(1000);

    SSD1306_Fill(0);
    y1 = 127, y2 = 0;
    while (y1 > 0)
    {
        SSD1306_DrawLine(y1, 0, y2, 63, 1);
        SSD1306_UpdateScreen();
        y1 -= 2;
        y2 += 2;
    }
    HAL_Delay(1000);

    SSD1306_Fill(1);
    SSD1306_UpdateScreen();
    SSD1306_DrawCircle(64, 32, 25, 0);
    SSD1306_UpdateScreen();
    SSD1306_DrawCircle(128, 32, 25, 0);
    SSD1306_UpdateScreen();
    SSD1306_DrawCircle(0, 32, 25, 0);
    SSD1306_UpdateScreen();
    SSD1306_DrawCircle(32, 32, 25, 0);
    SSD1306_UpdateScreen();
    SSD1306_DrawCircle(96, 32, 25, 0);
    SSD1306_UpdateScreen();
    HAL_Delay(1000);

    SSD1306_Fill(0);
    SSD1306_UpdateScreen();
    int32_t i = -100;
    char buf[10];
    while (i <= 100)
    {
        memset(&buf[0], 0, sizeof(buf));
        sprintf(buf, "%d", i);
        SSD1306_GotoXY(50, 27);
        SSD1306_Puts(buf, &Font_6x10, 1);
        SSD1306_DrawLine(64, 10, (i + 100) * 128 / 200, (i + 100) * 64 / 200, 1);
        SSD1306_UpdateScreen();
        SSD1306_Fill(0);
        i++;
    }
    SSD1306_GotoXY(50, 27);
    sprintf(buf, "END");
    SSD1306_Puts(buf, &Font_6x10, 1);
    SSD1306_UpdateScreen();
}

void I2C_ErrorHandler(void)
{
    while (1);
}

void APP_I2C_Transmit(uint8_t devAddress, uint8_t memAddress, uint8_t *pData, uint16_t len)
{
  HAL_I2C_Mem_Write(&I2cHandle, devAddress, memAddress, I2C_MEMADD_SIZE_8BIT, pData, len, 5000);
}


void OLED_DisplayFlashingText(const char *message)
{
    for (int i = 0; i < 10; i++) {  // Flash the text 10 times, adjust this loop as needed
        OLED_SetTextColor(RED_TEXT);  // Set text color to red (or any desired color)
        OLED_DisplayMessage((char*)message);  // Display the message
        HAL_Delay(500);  // Wait 500ms (adjust delay as needed)

        OLED_Clear();  // Clear the text (or set the text color to background color)
        HAL_Delay(500);  // Wait another 500ms to complete the flash
    }
}

void OLED_DisplayMessageWithColor(const char *message, uint32_t color)
{
    SSD1306_Fill(color);
    OLED_DisplayMessage(message);
}

void OLED_DisplayText(uint8_t x, uint8_t y, char *text)
{
    SSD1306_GotoXY(x, y);
    SSD1306_Puts(text,&Font_6x10, 1);
    SSD1306_UpdateScreen();
}


void OLED_DisplayPotValue(uint32_t val)
{
    static uint32_t lastDisplayVal = 0;

    uint32_t voltage = (val * 3300) / 4096;  // Convert the 12-bit ADC value to a voltage (mV)

    uint32_t ppm;
    if (voltage <= 1500) {
        ppm = (voltage - 1000) * 100 / 500;
    } else if (voltage <= 2500) {
        ppm = 100 + (voltage - 1500) * 400 / 1000;
    } else {
        ppm = 500;  // Cap it at 500 ppm as the sensor's max range
    }

    float bac = ppm / 2100.0f;

    if ((ppm > lastDisplayVal ? ppm - lastDisplayVal : lastDisplayVal - ppm) > 10) {
        lastDisplayVal = ppm;

        char data[20];
        snprintf(data, sizeof(data), "BAC: %.4f", bac);
        SSD1306_GotoXY(50, 0);
        SSD1306_Puts(data, &Font_6x10, 1);
        SSD1306_UpdateScreen();
        //OLED_Clear();
        //OLED_DisplayText(0, 25, data);
    }
}

void OLED_DisplayMessage(const char *message)
{
    SSD1306_GotoXY(0,0);
    SSD1306_Puts((char*)message, &Font_6x12, 1);
    SSD1306_UpdateScreen(); // display
}

void OLED_SetTextColor(uint32_t color)
{
    SSD1306_Fill(color);
    SSD1306_UpdateScreen();
}

void OLED_ClearRow(uint8_t y)
{
    for (uint8_t x = 0; x < 128; x++) {
        SSD1306_DrawPixel(x,y, SSD1306_COLOR_BLACK);      
    }
    SSD1306_UpdateScreen(); 
}
void OLED_Clear()
{
    /* Clear screen */
    SSD1306_Fill(SSD1306_COLOR_BLACK);

    /* Update screen */
    SSD1306_UpdateScreen();
}