import sys
import os
import subprocess
from PyQt5.QtWidgets import (QApplication, QMainWindow, QAction, QWidget, QVBoxLayout, QPushButton, QComboBox, QLabel, QProgressBar, QGroupBox, QCompleter, QFileDialog)
from PyQt5.QtCore import Qt, QThread, pyqtSignal

# Model
import subprocess
from PyQt5.QtCore import QThread, pyqtSignal

class ETLModel(QThread):
    finished = pyqtSignal()
    progress = pyqtSignal(int)
    status = pyqtSignal(str)

    def __init__(self, manufacturer, operationType, filePath=None):
        super().__init__()
        self.manufacturer = manufacturer
        self.operationType = operationType
        self.filePath = filePath

    def run(self):
        script_path = self.get_script_path_by_manufacturer(self.manufacturer)
        if script_path:
            self.status.emit(f"Starting {self.manufacturer} ETL process...")
            try:
                # If filePath is provided, pass it to the script as an argument
                command = ['python', script_path] + ([self.filePath] if self.filePath else [])
                result = subprocess.run(command, capture_output=True, text=True)
                
                if result.returncode == 0:
                    self.status.emit("ETL Process Completed Successfully")
                else:
                    self.status.emit(f"ETL Process Failed: {result.stderr}")
            except Exception as e:
                self.status.emit(f"ETL Process Error: {e}")
        else:
            self.status.emit("No script found for selected manufacturer.")
        self.finished.emit()

    def get_script_path_by_manufacturer(self, manufacturer):
        # Dictionary mapping manufacturers to their specific ETL script paths
        scripts = {
            "GANESH MILLS": "C:\\Users\\juddu\\OneDrive\\Mission Critical Projects\\Vendor Onboarding APP\\VBA Application\\Module_GaneshMills.py",
            # Add other manufacturers and their script paths here
        }
        return scripts.get(manufacturer, None)
class TransformationModel(QThread):
    mappingFinished = pyqtSignal(dict)

    def __init__(self, manufacturer, parent=None):
        super().__init__(parent)  # Pass the parent to QThread's constructor
        self.manufacturer = manufacturer

    def run(self):
        # Placeholder for transformation logic
        # Simulate fetching or generating mapping data
        mapping_data = self.get_mapping_data(self.manufacturer)
        self.mappingFinished.emit(mapping_data)

    def get_mapping_data(self, manufacturer):
        # Placeholder for actual logic to retrieve mapping data
        # This example returns dummy data
        return {"Column A": "Transformed A", "Column B": "Transformed B"}



# View
class DataView(QWidget):
    filePathSelected = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.operationTypes = ["Inventory Upload", "Price Changes", "New Products"]
        self.currentOperationIndex = 0
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        etlGroupBox = QGroupBox("ETL Process Configuration")
        etlLayout = QVBoxLayout()
        etlLayout.setSpacing(10)
        etlLayout.setContentsMargins(10, 20, 10, 10)

        self.manufacturerComboBox = QComboBox()
        self.manufacturerComboBox.setEditable(True)
        manufacturers = ["Choose Supplier", "GANESH MILLS", "WESTPOINT HOSPITALITY", "THOMASTON MILLS", "HOSPITALITY 1 SOURCE", "DOWNLITE", "1888 MILLS", "BERKSHIRE HOSPITALITY", "HOLLYWOOD BED FRAME", "CSL", "KARTRI", "FORBES", "SICO", "BISSEL", "HAPCO", "JS FIBER", "KTX", "PACIFIC COAST", "GLARO", "CONAIR", "ESSENDENT"]
        self.manufacturerComboBox.addItems(manufacturers)
        completer = QCompleter(manufacturers[1:])  # Exclude placeholder from completer
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        self.manufacturerComboBox.setCompleter(completer)
        etlLayout.addWidget(self.manufacturerComboBox)

        self.operationTypeButton = QPushButton(self.operationTypes[self.currentOperationIndex])
        self.operationTypeButton.clicked.connect(self.toggleOperationType)
        etlLayout.addWidget(self.operationTypeButton)


        self.progressBar = QProgressBar()
        self.progressBar.setMaximum(100)
        etlLayout.addWidget(self.progressBar)

        self.selectFileButton = QPushButton('Select Data Feed File')
        self.selectFileButton.clicked.connect(self.selectFile)
        etlLayout.addWidget(self.selectFileButton)
       
        self.startETLButton = QPushButton('Start ETL Process')
        etlLayout.addWidget(self.startETLButton)

        self.filePathLabel = QLabel("No file selected")
        etlLayout.addWidget(self.filePathLabel)
        
        self.statusLabel = QLabel("Ready")
        etlLayout.addWidget(self.statusLabel)

        etlGroupBox.setLayout(etlLayout)
        layout.addWidget(etlGroupBox)
        self.setLayout(layout)
        
        # Setup for the Mapping tab or section
        self.mappingLayout = QVBoxLayout()
        self.mappingLabel = QLabel("Mapping information will be displayed here.")
        self.mappingLayout.addWidget(self.mappingLabel)
        
        # Assuming etlGroupBox is a part of your main layout
        self.mappingGroupBox = QGroupBox("Mapping Information")
        self.mappingGroupBox.setLayout(self.mappingLayout)
        layout.addWidget(self.mappingGroupBox)

    def selectFile(self):
        filePath, _ = QFileDialog.getOpenFileName(self, "Select Data Feed File", "", "Data Files (*.csv;*.xlsx;*.json)")
        if filePath:
            self.filePathLabel.setText(os.path.basename(filePath))
            self.filePathSelected.emit(filePath)

    def toggleOperationType(self):
        self.currentOperationIndex = (self.currentOperationIndex + 1) % len(self.operationTypes)
        self.operationTypeButton.setText(self.operationTypes[self.currentOperationIndex])

    def updateMappingInfo(self, mapping_data):
        mapping_text = "Mapping Information:\n" + "\n".join([f"{k}: {v}" for k, v in mapping_data.items()])
        self.mappingLabel.setText(mapping_text)

class DataController:
    def __init__(self, view):
        self.view = view
        self.filePath = None
        self.view.filePathSelected.connect(self.setFilePath)
        self.view.startETLButton.clicked.connect(self.startETLProcess)

    def setFilePath(self, filePath):
        self.filePath = filePath

    def startETLProcess(self):
        manufacturer = self.view.manufacturerComboBox.currentText()
        operationType = self.view.operationTypeButton.text()

        if manufacturer == "Choose Supplier" or not self.filePath:
            self.view.statusLabel.setText("Please choose a supplier and a file.")
            return

        self.etlModel = ETLModel(manufacturer, operationType, self.filePath)
        self.etlModel.progress.connect(self.view.progressBar.setValue)
        self.etlModel.status.connect(self.view.statusLabel.setText)
        self.etlModel.finished.connect(self.onETLFinished)
        self.etlModel.start()
        self.view.startETLButton.setEnabled(False)

    def onETLFinished(self):
        # Directly start the mapping process when ETL finishes
        self.startMappingProcess()

    def startMappingProcess(self):
        manufacturer = self.view.manufacturerComboBox.currentText()
        # Initialize the transformation model and connect signals
        self.transformationModel = TransformationModel(manufacturer)
        self.transformationModel.mappingFinished.connect(self.view.updateMappingInfo)
        self.transformationModel.start()
        self.view.statusLabel.setText("ETL Process Completed. Starting Mapping...")

# Main Window
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('DATA COMBINER APPLICATION')
        self.setGeometry(300, 300, 800, 600)
        self.initUI()

    def initUI(self):
        self.view = DataView(self)
        self.controller = DataController(self.view)
        self.setCentralWidget(self.view)

        self.statusBar().showMessage('Ready')
        menuBar = self.menuBar()
        Supplier_OnboardingMenu = menuBar.addMenu('&Supplier OnBoarding')
        ProductsMenu = menuBar.addMenu('&Products')        
        MappingMenu = menuBar.addMenu('&Mapping')
        SettingsMenu = menuBar.addMenu('&Settings')
        ExitMenu = menuBar.addMenu('&Exit')
        exitAction = QAction('&Exit', self)
        exitAction.setShortcut('Ctrl+Q')
        exitAction.triggered.connect(self.close)
        ExitMenu.addAction(exitAction)

# Main Execution
if __name__ == '__main__':
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    mainWindow.show()
    sys.exit(app.exec_())
