from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QThread, pyqtSignal

import logging
from pathlib import Path
import os
from functools import lru_cache

from PandasModel import PandasModel
import pandas as pd
import csv
from io import StringIO
import re
from functools import partial

FORMAT = '%(asctime)s - %(name)20s - %(funcName)20s - %(levelname)8s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)


class Widget(QtWidgets.QWidget):
    def __init__(self, parent=None):

        QtWidgets.QWidget.__init__(self, parent=None)

        self.initUI()

        self.chunksize = 2 * 1024 ** 2
        self.linechunk = 10000
        self.linenumseparator = '\t'

        self.file_index_thread = FileIndex(filename='', linechunk=self.linechunk)
        self.file_index_thread.signal.connect(self._file_index)

        self.line_count_thread = LineCount(filename='')
        self.line_count_thread.signal.connect(self._total_lines)

        self.search_index_thread = SearchIndex(filename='')
        self.search_index_thread.signal.connect(self._search_index)

        self.fileName = None
        self.fileFormat = None
        self.filesize = None
        self.file_index = None
        self.filelength = None #length of file in bytes
        self.chunklines = None
        self.linesize = None
        self.headsize = None
        self.estimated_lines = None
        self.total_lines = None
        self.header = None
        self.has_header = None
        self.delimiter = None
        self.dialect = None
        self.currentstartline = None #row number of the current chunk
        self.searchindex = None
        self.line_numbers = self.linesnumberCheck.isChecked

    def initUI(self):

        self.left = 500
        self.top = 500
        self.width = 800
        self.height = 500
        self.setGeometry(self.left, self.top, self.width, self.height)

        vLayout = QtWidgets.QVBoxLayout(self)
        hLayout = QtWidgets.QHBoxLayout()
        hLayoutText = QtWidgets.QHBoxLayout()

        self.setWindowTitle("Large File Reader")
        self.setWindowIcon(QtGui.QIcon(r'P:\pyprojects\largefileviewer\resources\images\baseline-arrow_right-24px.svg'))

        # self.pathLE = QtWidgets.QLineEdit(self)
        # hLayout.addWidget(self.pathLE)

        self.loadBtn = QtWidgets.QPushButton("Select File", self)
        self.loadBtn.setMinimumWidth(80)
        self.loadBtn.setMaximumWidth(80)
        hLayout.addWidget(self.loadBtn)
        self.loadBtn.clicked.connect(self.loadFile)

        self.firstBtn = QtWidgets.QPushButton("First", self)
        hLayout.addWidget(self.firstBtn)
        self.firstBtn.clicked.connect(self.loadFirst)
        self.firstBtn.setMinimumWidth(40)
        self.firstBtn.setMaximumWidth(40)
        self.firstBtn.setEnabled(False)

        self.lastBtn = QtWidgets.QPushButton("Last", self)
        hLayout.addWidget(self.lastBtn)
        self.lastBtn.clicked.connect(self.loadLast)
        self.lastBtn.setMinimumWidth(40)
        self.lastBtn.setMaximumWidth(40)
        self.lastBtn.setEnabled(False)

        self.linenumberEdit = QtWidgets.QLineEdit(self)
        self.linenumberEdit.setMinimumWidth(20)
        self.linenumberEdit.setMaximumWidth(80)
        hLayout.addWidget(self.linenumberEdit)

        self.gotoBtn = QtWidgets.QPushButton("Goto", self)
        hLayout.addWidget(self.gotoBtn)
        self.gotoBtn.setMinimumWidth(50)
        self.gotoBtn.setMaximumWidth(50)
        self.gotoBtn.clicked.connect(partial(self.loadLine, self.linenumberEdit.text()))
        self.gotoBtn.setEnabled(False)

        self.linesnumberCheck = QtWidgets.QCheckBox("Line Numbers")
        hLayout.addWidget(self.linesnumberCheck)

        self.searchEdit = QtWidgets.QLineEdit(self)
        self.searchEdit.setMinimumWidth(40)
        self.searchEdit.setMaximumWidth(300)
        hLayout.addWidget(self.searchEdit)

        self.searchBtn = QtWidgets.QPushButton("Search", self)
        hLayout.addWidget(self.searchBtn)
        self.searchBtn.setMinimumWidth(50)
        self.searchBtn.setMaximumWidth(50)
        self.searchBtn.clicked.connect(self.search)
        self.searchBtn.setEnabled(False)


        self.rawBtn = QtWidgets.QPushButton("Show as file", self)
        hLayout.addWidget(self.rawBtn)
        self.rawBtn.setCheckable(True)
        self.rawBtn.clicked.connect(lambda: None)
        self.rawBtn.setMinimumWidth(80)
        self.rawBtn.setMaximumWidth(80)
        self.rawBtn.setEnabled(False)
        self.rawBtn.clicked.connect(self.toggleTextWnd)

        self.tableBtn = QtWidgets.QPushButton("Show as table", self)
        hLayout.addWidget(self.tableBtn)
        self.tableBtn.setCheckable(True)
        self.tableBtn.clicked.connect(self._show_as_table)
        self.tableBtn.setMinimumWidth(80)
        self.tableBtn.setMaximumWidth(80)
        self.tableBtn.setEnabled(False)

        vLayout.addLayout(hLayout)

        # self.linewnd = QtWidgets.QTextEdit(self)
        # hLayoutText.addWidget(self.linewnd)
        # self.linewnd.setReadOnly(True)
        # self.linewnd.setWordWrapMode(False)
        # self.linewnd.setFont(QtGui.QFont('Courier New', 10))
        # self.linewnd.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        # self.linewnd.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        # self.linewnd.setMaximumWidth(60)

        self.textwnd = QtWidgets.QTextEdit(self)
        hLayoutText.addWidget(self.textwnd)
        self.textwnd.setReadOnly(True)
        self.textwnd.setWordWrapMode(False)
        self.textwnd.setFont(QtGui.QFont('Courier New', 10))
        self.textwnd.ensureCursorVisible()
        self.textwnd.setAcceptDrops(True)


        self.pandasTv = QtWidgets.QTableView(self)
        self.pandasTv.setSortingEnabled(True)
        #self.pandasTv.setFont(QtGui.QFont('Courier New', 8))
        self.pandasTv.setFont(QtGui.QFont('Arial', 8))
        self.pandasTv.setWordWrap(False)
        self.pandasTv.setMinimumWidth(0)
        self.pandasTv.setMaximumWidth(0)

        hLayoutText.addWidget(self.pandasTv)

        vLayout.addLayout(hLayoutText)

        self.statusBar = QtWidgets.QStatusBar()
        vLayout.addWidget(self.statusBar)


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

    def _line_numbers(self, text, linestart=0, return_as_text=True):
        lineno = [n + linestart for n, _ in enumerate(text.split('\n'))]
        if return_as_text:
            strlines = [str(x) for x in lineno]
            lines = '\n'.join(strlines)
            return lines
        else:
            return lineno

    def _add_line_numbers(self, text, linestart=0):
        a = [[str(n + linestart), self.linenumseparator, _] for n, _ in enumerate(text.split('\n'))]
        c = [''.join(b) for b in a]
        d = '\n'.join(c)
        return d

    def reset_fileproperties(self):
        self.dialect = None
        self.delimiter = None
        self.has_header = None
        self.header = None
        self.file_index = None
        self.total_lines = None
        self.estimated_lines = None
        self.currentstartline = None
        self.chunklines = None
        self.searchindex = None

    def set_fileproperties(self, text):

        row = text[:text.find("\n")]
        samplerows = text[:text.find("\n", 5)]

        try:
            # self.dialect = csv.Sniffer().sniff(toprows[0], delimiters=["\t", ",", ";", "|", "||", "~"])
            self.dialect = csv.Sniffer().sniff(row, delimiters=["\t", ",", ";", "|", "||", "~"])
        except Exception as e:
            logger.warning(e)
        logger.debug(f"Dialect is {self.dialect}")
        self.delimiter = self.dialect.delimiter
        logger.debug(f"Delimiter is: {ord(self.delimiter)}")
        self.has_header = csv.Sniffer().has_header(samplerows)
        logger.debug(f"Has header: {self.has_header}")
        self.header = row.split(self.delimiter)
        if len(self.header) < 2:
            logger.warning("Header is likely not properly separated.")
        logger.debug(f"Header is: {self.header}")

    def loadFirst(self):
        """Load first chunk of bytes"""
        logger.debug("loadFirst")

        if self.filelength <= self.chunksize:
            logger.debug(f"EOF {self.filelength} <= chunksize {self.chunksize}")
            text = self.reader(self.fileName, 0, os.SEEK_SET, nbytes=None)
        else:
            logger.debug(f"EOF {self.filelength} >  chunksize{self.chunksize}")
            text = self.reader(self.fileName, 0, os.SEEK_SET, nbytes=self.chunksize)

        if not self.delimiter: # determine once the basic properties of this file, such as delimtier, quoting etc.
            self.set_fileproperties(text)

        self.currentstartline = 0
        if self.line_numbers():
            text = self._add_line_numbers(text, linestart=self.currentstartline)
        self.textwnd.setText(text)

        if self.tableBtn.isChecked():
            self._show_as_table()

    def loadLast(self):
        """Load last chunk of bytes"""
        logger.debug("loadLast")
        if self.filelength >= self.chunksize:
            logger.debug(f"EOF {self.filelength} >=  chunksize {self.chunksize}")
            text = self.reader(self.fileName, -self.chunksize, from_what=os.SEEK_END, nbytes=self.chunksize)
        else:
            logger.debug(f"EOF {self.filelength} chunksize < {self.chunksize}")
            text = self.reader(self.fileName, 0, from_what=os.SEEK_SET)

        lastchunklines = self._chunklines()
        logger.debug(f"Lines in the last chunk: {lastchunklines}")

        if self.total_lines:
            logger.debug(f"Total lines are known: {self.total_lines}")
            self.currentstartline = self.total_lines - lastchunklines
        else:
            logger.debug(f"Lines are estimated: {self.estimated_lines}")
            self.currentstartline = self.estimated_lines - lastchunklines
        logger.debug(f"Last chunk startline: {self.currentstartline }")

        if self.line_numbers():
            text = self._add_line_numbers(text, linestart=self.currentstartline)

        self.textwnd.setText(text)
        self.textwnd.moveCursor(QtGui.QTextCursor.End)

        if self.tableBtn.isChecked():
            self._show_as_table()
            #self.pandasTv.moveCursor(QtGui.QTextCursor.End)

    def currentchunk(self, linenumber):
        """Given a line number, return the current index chunk to load"""

        if linenumber > self.total_lines:
            logger.warning("Line number requested is greater than total lines in file. Returning last line.")
            linenumber = self.total_lines
        elif linenumber < 0:
            logger.warning("Line number requested is smaller than 0. Returning first line.")
            linenumber = 0

        logger.debug(f"Loading line {linenumber}")
        if linenumber <  self.linechunk:
            lineschunk = 0
        else:
            lineschunk = linenumber // self.linechunk * self.linechunk + 1
        return lineschunk

    def loadLine(self, linenumber):
        """Move to  record in file"""
        logger.debug("loadLine")
        import pdb; pdb.set_trace()


        self.currentstartline = self.currentchunk(linenumber) #find index chunk in which the line is located
        logger.debug(f"Loading from index position {self.currentstartline}")

        byteposition = self.file_index[self.currentstartline] #must be an index key
        logger.debug(f"Byteposition in file {byteposition}")
        text = self.reader(self.fileName, byteposition, from_what=os.SEEK_SET, nbytes=self.chunksize)
        # self.linewnd.setText(self._line_numbers(text, linenumber))
        if self.line_numbers():
            text = self._add_line_numbers(text, linestart=self.currentstartline)
        self.textwnd.setText(text)

        # move cursor to selected line
        offset = linenumber % self.linechunk - 1
        logger.debug(f"Moving {offset} in current text window.")
        cursor = self.textwnd.textCursor()
        cursor.movePosition(QtGui.QTextCursor.Down, QtGui.QTextCursor.MoveAnchor, offset)
        self.textwnd.setTextCursor(cursor)

        if self.tableBtn.isChecked():
            self._show_as_table()
            #cursor = self.pandasTv.textCursor()
            #cursor.movePosition(QtGui.QTextCursor.Down, QtGui.QTextCursor.MoveAnchor, offset)
            #self.pandasTv.setTextCursor(cursor)

    def loadFile(self):
        """Load the file from file picker"""
        logger.debug("loadFile")
        fileName, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Open File", "", "All Files (*);;CSV Files (*.csv);;TSV Files (*.txt; *.tsv);;Parquet Files (*.parc; *.parquet)");


        if self.file_index_thread.isRunning():
            self.file_index_thread.terminate()
        if self.line_count_thread.isRunning():
            self.line_count_thread.terminate()
        if self.search_index_thread.isRunning():
            self.search_index_thread.terminate()

        self.reset_fileproperties()

        self.fileName = fileName
        logger.debug(f"File name: {fileName}")

        # self.pathLE.setText(self.fileName)
        self.setWindowTitle("Large File Reader " + self.fileName)
        self.file_index_thread.filename = self.fileName
        self.line_count_thread.filename = self.fileName
        self.search_index_thread.filename = self.fileName

        self.fileFormat = Path(self.fileName).suffix
        logger.debug("File format is {}".format(self.fileFormat))

        self.filelength = self._filelength()
        logger.debug("File length in bytes is {}".format(self.filelength))

        self.loadFirst()
        self.estimate_lines()

        self.chunklines = self._chunklines()

        self.file_index_thread.start()
        self.line_count_thread.start()
        self.search_index_thread.start()

        self.rawBtn.toggle()  # toggle view as file button
        self.rawBtn.setEnabled(True)
        self.lastBtn.setEnabled(True)
        self.firstBtn.setEnabled(True)
        self.tableBtn.setEnabled(True)

    def toggleTextWnd(self):
        if self.rawBtn.isChecked():
            self.textwnd.setVisible(True)
        else:
            self.textwnd.setVisible(False)

    def estimate_lines(self):
        """Estimate line count without iterating through file"""
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
        self.gotoBtn.setEnabled(True)
        logger.debug("File index created: {}".format(self.file_index))

    def _first_dict_times(self, mydict, n):
        i = 0
        shortdict = {}
        for k, v in mydict.items():
            i += 1
            shortdict[k] = v
            if i == n:
                return shortdict


    def _search_index(self, result):
        self.searchindex = result
        self.searchBtn.setEnabled(True)
        import pdb; pdb.set_trace()
        searchindexstring = self._first_dict_times(self.searchindex, 5)
        logger.debug("Search index created: {}".format(str(searchindexstring) + "..."))

    def search(self):
        searchterm = self.searchEdit.text()
        foundlines = self.searchindex.get(searchterm, None)
        if foundlines:
            self.statusBar.showMessage(f"{searchterm} is in lines {foundlines}.")
        else:
            self.statusBar.showMessage(f"{searchterm} not found.")

    def _show_as_table(self):

        if self.tableBtn.isChecked():
            self.pandasTv.show()

            #if self.pandasTv.isHidden():
            #    self.pandasTv.setVisible(True)
            text = self.textwnd.toPlainText()
            #lineno = self._line_numbers(text=text, linestart=self.currentstartline, return_as_text=False)
            self.pandasTv.setMaximumWidth(10000)
            mydata = StringIO(text)
            df = pd.read_csv(mydata, dialect=self.dialect, names=self.header, error_bad_lines=False)
            # TODO fix the index in the table view. update the index without erroring in pandas model
            #if self.total_lines:
            #    rngIndex = pd.RangeIndex(start=self.currentstartline, stop=self.currentstartline + len(df), step=1)
            #    df.index = rngIndex
            #    logger.debug(rngIndex)
            #    df.index = rngIndex
            #import pdb; pdb.set_trace()
            #logger.debug(df.index)
            #logger.debug(df.head())
            df = df.fillna('')
            model = PandasModel(df)
            self.pandasTv.setModel(model)
        else:
            self.pandasTv.hide()

    @lru_cache(8)
    def reader(self, file, offset, from_what=os.SEEK_SET, nbytes=None):
        """
        Read bytes from file using offsets

        https://stackoverflow.com/questions/11696472/seek-function
        :param offset:
        :param from_what:
        :param nbytes:
        :return:
        """

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
        logger.debug(f"Reading chunk  of {read_bytes} bytes at  {offset} from {read_position}")

        with open(file, 'rb') as myfile:
            myfile.seek(offset, from_what)
            text = myfile.read(nbytes)
            try:
                text = text.decode("utf-8")
            except Exception as e:
                logger.warning(f"Could not encode file in unicode. {e}")
                text = text.decode("latin1")

        #make unix compatible
        text = text.replace("\r\n", "\n")

        if from_what == os.SEEK_END:
            firstlineend = text.find("\n", 1)
            text = text[firstlineend + 1:]
        elif from_what == os.SEEK_CUR:
            lastlinenend = text.rfind("\n")
            text = text[:lastlinenend]

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
            fileindex = {n: myfile.tell() for n, _ in enumerate(iter(myfile.readline, '')) if n % self.linechunk == 1}
        fileindex.pop(1, None)
        fileindex[0] = 0
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

class SearchIndex(QThread):
    """Index the lines in a large file in a separate thread"""
    signal = pyqtSignal('PyQt_PyObject')

    def __init__(self, filename):
        QThread.__init__(self)
        self.filename = filename

    def run(self):
        logger.debug("Creating search index...")
        assert self.filename != ''
        do_not_index={}
        searchindex = {}
        itemsindexed = 0
        with open(self.filename, 'r', encoding="utf-8") as myfile:

            for ln, line in enumerate(myfile):
                terms = re.findall(r"[\w'\-[0-9]+", line)
                for t in terms:
                    if not do_not_index.get(t, False):
                        searchindex[t] = list(set(searchindex.get(t, []) + [ln])) #store line in which each searchindex lives
                        if len(searchindex[t]) > 50:
                            do_not_index[t] = True
                if len(searchindex.keys()) % 100000 == 0:
                    itemsindexed += 100000
                    logger.debug(f"{itemsindexed} indexed.")
        logger.debug(f"Creating search index...Done. {len(searchindex.keys())} items indexed.")
        self.signal.emit(searchindex)


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    app.processEvents()
    w = Widget()
    w.show()

    timer = QtCore.QTimer()
    timer.timeout.connect(lambda: None)
    timer.start(100)

    sys.exit(app.exec_())