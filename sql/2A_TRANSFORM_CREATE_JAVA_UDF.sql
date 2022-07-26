--create stage to store the jar file 
CREATE STAGE jars_stage;
-- #### RUN the below command via SnowSQL to uplaod data to your Stage ####
--put file://pdfbox-app-2.0.25.jar @jars_stage  AUTO_COMPRESS=false;
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

-- #####################################
-- ## Process the files with our UDF
-- ###################################

--this process is quite memory intensive. Lets use the power of Snowflake to instantly scale to process all the PDFs
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = '2X-Large';
--save all the PDF data along with the file name into a new table in its text format 
CREATE TRANSIENT TABLE PDF_RAW_TEXT AS 
(select RELATIVE_PATH, process_pdf(file_url) as Pdf_text
from (
    select file_url,RELATIVE_PATH
    from directory(@pdf_file_stage)
    group by file_url, RELATIVE_PATH
)) ;
--scale the compute back down once complete
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'X-Small';