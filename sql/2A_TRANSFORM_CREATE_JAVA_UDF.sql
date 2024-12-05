--create stage to store the jar file 
CREATE STAGE jars_stage;
-- #### RUN the below command via SnowSQL to uplaod data to your Stage ####
--put file://pdfbox_jar/pdfbox-app-2.0.25.jar @jars_stage  AUTO_COMPRESS=false;
--#####

--PDF function which takes in a PDF file and returns extracted text
CREATE FUNCTION process_pdf(file string)
RETURNS string
LANGUAGE java
RUNTIME_VERSION = 11
IMPORTS = ('@jars_stage/pdfbox-app-2.0.25.jar')
HANDLER = 'PdfParser.readFile'
as
$$
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import com.snowflake.snowpark_java.types.SnowflakeFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
public class PdfParser {
public static String readFile(String fileURL) throws IOException {
        SnowflakeFile file = SnowflakeFile.newInstance(fileURL);
        try (PDDocument document = PDDocument.load(file.getInputStream())) {
document.getClass();
if (!document.isEncrypted()) {
PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                stripper.setSortByPosition(true);
PDFTextStripper tStripper = new PDFTextStripper();
String pdfFileInText = tStripper.getText(document);
                return pdfFileInText;
            }
        }
return null;
    }
}
$$;

--Can also use Python UDF
CREATE FUNCTION IF NOT EXISTS pdf_to_text_udf(file_path string)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pypdf2')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from PyPDF2 import PdfFileReader
from io import BytesIO
 
def main(file_path):
    with SnowflakeFile.open(file_path, 'rb') as f:
        f = BytesIO(f.readall())
        reader = PdfFileReader(f)
        pageObj = reader.pages[0]
        text = pageObj.extractText()
        return text
$$;


