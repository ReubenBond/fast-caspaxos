pdf_input=$1
ps_output=${pdf_input%.*}.ps
pdfa_output=${pdf_input%.*}_a.pdf
pdftops ${pdf_input} ${ps_output}

gs -sDEVICE=pdfwrite -dBATCH -dNOPAUSE -dSAFER -sColorConversionStrategy=UseDeviceIndependentColor -dEmbedAllFonts=true -dPrinted=true -dPDFA -sProcessColorModel=DeviceRGB -dPDFACompatibilityPolicy=1 -dDetectDuplicateImages -r150 -dFastWebView=true -sOutputFile=${pdfa_output} ${ps_output}

