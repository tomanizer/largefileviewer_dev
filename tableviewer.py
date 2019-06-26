from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QThread, pyqtSignal

import logging
from pathlib import Path
import os
from functools import lru_cache

FORMAT = '%(asctime)s - %(name)20s - %(funcName)20s - %(levelname)8s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)


class Widget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent=None)
        vLayout = QtWidgets.QVBoxLayout(self)
        hLayout = QtWidgets.QHBoxLayout()

        self.setWindowTitle("Large File Reader")

        self.pathLE = QtWidgets.QLineEdit(self)
        hLayout.addWidget(self.pathLE)

        self.loadBtn = QtWidgets.QPushButton("Select File", self)
        hLayout.addWidget(self.loadBtn)
        self.loadBtn.clicked.connect(self.loadFile)

        self.firstBtn = QtWidgets.QPushButton("First", self)
        hLayout.addWidget(self.firstBtn)
        self.firstBtn.clicked.connect(self.loadFirst)

        self.lastBtn = QtWidgets.QPushButton("Last", self)
        hLayout.addWidget(self.lastBtn)
        self.lastBtn.clicked.connect(self.loadLast)

        self.linenumberEdit = QtWidgets.QLineEdit(self)
        self.linenumberEdit.setMinimumWidth(20)
        self.linenumberEdit.setMaximumWidth(80)
        hLayout.addWidget(self.linenumberEdit)

        self.gotoBtn = QtWidgets.QPushButton("Goto", self)
        hLayout.addWidget(self.gotoBtn)
        self.gotoBtn.clicked.connect(self.loadLine)

        vLayout.addLayout(hLayout)

        self.textwnd = QtWidgets.QTextEdit(self)
        vLayout.addWidget(self.textwnd)
        self.textwnd.setReadOnly(True)
        self.textwnd.setWordWrapMode(False)
        self.textwnd.setFont(QtGui.QFont('Courier New', 10))

        self.statusBar = QtWidgets.QStatusBar()
        vLayout.addWidget(self.statusBar)

        self.chunksize = 5 * 1024 ** 2
        self.linechunk = 10000

        self.file_index_thread = FileIndex(filename='', linechunk=self.linechunk)
        self.file_index_thread.signal.connect(self._file_index)

        self.line_count_thread = LineCount(filename='')
        self.line_count_thread.signal.connect(self._total_lines)

        self.fileName = None
        self.fileFormat = None
        self.filesize = None
        self.filelength = None #length of file in bytes
        self.chunklines = None
        self.linesize = None
        self.headsize = None
        self.estimated_lines = None
        self.total_lines = None

    def _chunklines(self):
        """How many lines of text are in one chunk of bytes"""
        text = self.textwnd.toPlainText()
        lines_in_chunk = len(text.split("\n"))
        logger.debug("Lines in chunk: {}".format(lines_in_chunk))
        return lines_in_chunk

    def _filelength(self):
        """How many bytes is the file long"""
        with open(self.fileName, 'rb') as f:
            f.seek(0, 2)  # move to end of file
            length = f.tell()  # get current position
        return length

    def loadFirst(self):
        """Load first chunk of bytes"""
        logger.debug("loadFirst")
        if self.filelength <= self.chunksize:
            logger.debug(f"EOF {self.filelength} <= chunksize {self.chunksize}")
            text = self.reader(0, os.SEEK_SET, nbytes=None)
        else:
            logger.debug(f"EOF {self.filelength} >  chunksize{self.chunksize}")
            text = self.reader(0, os.SEEK_SET, nbytes=self.chunksize)
        self.textwnd.setText(text)
        self.statusBar.showMessage(f"First loaded")

    def loadLast(self):
        """Load last chunk of bytes"""
        logger.debug("loadLast")
        if self.filelength >= self.chunksize:
            logger.debug(f"EOF {self.filelength} >=  chunksize {self.chunksize}")
            text = self.reader(-self.chunksize, from_what=os.SEEK_END, nbytes=self.chunksize)
        else:
            logger.debug(f"EOF {self.filelength} chunksize < {self.chunksize}")
            text = self.reader(0, from_what=os.SEEK_SET)
        self.textwnd.setText(text)
        self.textwnd.moveCursor(QtGui.QTextCursor.End)
        self.statusBar.showMessage(f"Last loaded")

    def loadLine(self ):
        """Move to  record in file"""
        logger.debug("loadLine")
        #import pdb; pdb.set_trace()
        linenumber = int(self.linenumberEdit.text())
        logger.debug(f"Loading line {linenumber}")
        lineschunk =  self.linechunk  % linenumber
        logger.debug(f"Loading from index position {lineschunk}")
        byteposition = self.file_index[lineschunk]
        logger.debug(f"Byteposition in file {byteposition}")
        text = self.reader(byteposition, from_what=os.SEEK_SET, nbytes=self.chunksize)
        self.textwnd.setText(text)
        self.statusBar.showMessage(f"Line loaded")
        #self.textwnd.moveCursor(QtGui.QTextCursor.End)


    def loadFile(self):
        """Load the file from file picker"""
        logger.debug("loadFile")
        fileName, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open File", "", "All Files (*);;CSV Files (*.csv);;TSV Files (*.txt; *.tsv);;Parquet Files (*.parc; *.parquet)");
        self.fileName = fileName
        logger.debug(f"File name: {fileName}")

        self.pathLE.setText(self.fileName)
        self.file_index_thread.filename = self.fileName
        self.line_count_thread.filename = self.fileName

        self.fileFormat = Path(self.fileName).suffix
        logger.debug("File format is {}".format(self.fileFormat))

        self.filelength = self._filelength()
        logger.debug("File length in bytes is {}".format(self.filelength))

        self.loadFirst()
        self.estimate_lines()

        self.chunklines = self._chunklines()

        self.file_index_thread.start()
        self.line_count_thread.start()

    def estimate_lines(self):
        """Estime line count without iterating through file"""
        logger.debug("estimate Lines")
        self.filesize = Path(self.fileName).stat().st_size
        text = self.textwnd.toPlainText()
        linetext = text.split("\n")[1] + "\\r\\n"
        self.linesize = len(linetext.encode('utf-8'))
        self.estimated_lines = self.filesize // self.linesize
        logger.debug("Estimate Lines: {}".format(self.estimated_lines))
        self.statusBar.showMessage(f"Estimated lines: {self.estimated_lines}")

    def _total_lines(self, result):
        self.total_lines = result
        self.statusBar.showMessage(f"Total lines: {self.total_lines}")

    def _file_index(self, result):
        self.file_index = result
        logger.debug("File index created: {}".format(self.file_index))


    @lru_cache(8)
    def reader(self, offset, from_what=os.SEEK_SET, nbytes=None):
        """
        Read bytes from file using offsets

        https://stackoverflow.com/questions/11696472/seek-function
        :param offset:
        :param from_what:
        :param nbytes:
        :return:
        """
        with open(self.fileName, 'rb') as myfile:
            if from_what == os.SEEK_SET:
                read_position = "start of file"
            elif from_what == os.SEEK_END:
                read_position = "end of file"
            elif from_what == os.SEEK_CUR:
                read_position = "current position in file"
            else:
                raise ValueError("from what must be valid os.SEEK type")
            if nbytes:
                read_bytes = f"{nbytes} bytes"
            else:
                read_bytes = f"all bytes"
            logger.debug(f"Reading {read_bytes} from {read_position}")
            myfile.seek(offset, from_what)
            text = myfile.read(nbytes)
            try:
                text = text.decode("utf-8")
            except Exception as e:
                logger.warning(f"Could not encode file in unicode. {e}")
                text = text.decode("latin1")
        return text


class FileIndex(QThread):
    """Index the lines in a large file in a separate thread"""
    signal = pyqtSignal('PyQt_PyObject')

    def __init__(self, filename, linechunk=10000):
        QThread.__init__(self)
        self.linechunk = linechunk
        self.filename = filename

    def run(self):
        logger.debug("Indexing file...")
        assert self.filename != ''
        with open(self.filename, 'r') as myfile:
            fileindex = {n: myfile.tell() for n, _ in enumerate(iter(myfile.readline, '')) if n % self.linechunk == 0}
            logger.debug("Indexing file...Done")
            self.signal.emit(fileindex)


class LineCount(QThread):
    """Index the lines in a large file in a separate thread"""
    signal = pyqtSignal('PyQt_PyObject')

    def __init__(self, filename):
        QThread.__init__(self)
        self.filename = filename

    def run(self):
        logger.debug("Counting lines in file...")
        assert self.filename != ''

        with open(self.filename, 'r') as myfile:
            for n, _ in enumerate(myfile):
                pass
        logger.debug(f"Counting lines in file...Done. {n} lines.")
        self.signal.emit(n)


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    w = Widget()
    w.show()
    sys.exit(app.exec_())