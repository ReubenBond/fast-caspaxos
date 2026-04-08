# --- Variables ---
FILENAME = main
TEX_DIR = sections
# Find all .tex files in the sections directory
TEX_FILES := $(wildcard $(TEX_DIR)/*.tex)

# Spellcheck settings
SPELLCHECK = aspell
SPELLCHECK_DICT = mywords.txt
# --mode=tex ensures aspell ignores LaTeX commands
SPELLCHECK_FLAGS = list -t --home-dir=. --personal=$(SPELLCHECK_DICT) --mode=tex
SPELLCHECK_OUT = spell_check

# Default values for dynamic assignment
JOBNAME = $(FILENAME)
TEX_PREAMBLE =

# --- Phony Targets ---
.PHONY: all pdf clean spell embed_font_check ieee acm usenix zip

# Default target: builds everything using the default settings
all: pdf embed_font_check spell 

# ======================================================================
# Target-specific variables
# ======================================================================
ieee: JOBNAME = $(FILENAME)_ieee
ieee: TEX_PREAMBLE = \def\ForceIEEE{1}
ieee: all

acm: JOBNAME = $(FILENAME)_acm
acm: TEX_PREAMBLE = \def\ForceACM{1}
acm: all

usenix: JOBNAME = $(FILENAME)_usenix
usenix: TEX_PREAMBLE = \def\ForceUSENIX{1}
usenix: all
# ======================================================================

# Build PDF using latexmk
pdf:
	latexmk -pdf -jobname=$(JOBNAME) -pdflatex='pdflatex -interaction=nonstopmode %O "$(TEX_PREAMBLE)\input{%S}"' $(FILENAME)
	# Embed fonts using Ghostscript (via ps2pdf)
	ps2pdf -dColorConversionStrategy=/LeaveColorUnchanged -dPDFSETTINGS=/prepress $(JOBNAME).pdf $(JOBNAME)_font_embedded.pdf

# Clean up temporary and generated files
clean:
	latexmk -C
	$(RM) *_font_embedded.pdf $(SPELLCHECK_OUT)
	$(RM) *.run.xml *.bcf
	# Clean up target-specific residual files
	$(RM) main_ieee* main_acm* main_usenix*

# Run spell check on all files in $(TEX_DIR)
spell:
	@$(RM) $(SPELLCHECK_OUT)
	@for file in $(TEX_FILES); do \
		echo "Checking typos in: $$file"; \
		echo "--- $$file ---" >> $(SPELLCHECK_OUT); \
		$(SPELLCHECK) $(SPELLCHECK_FLAGS) < $$file >> $(SPELLCHECK_OUT); \
	done

# Check and compare font embedding status
embed_font_check:
	@echo "Checking fonts for original PDF:"
	pdffonts $(JOBNAME).pdf
	@echo ""
	@echo "Checking fonts for embedded PDF:"
	pdffonts $(JOBNAME)_font_embedded.pdf

# ======================================================================
# Create a zip archive for submission (arXiv, Camera-ready, etc.)
# ======================================================================
# The zip command packages only the necessary source files.
# It runs 'clean' first to ensure no junk files are included.
zip: clean
	@echo "Creating zip archive: source_$(JOBNAME).zip"
	@$(RM) source_$(JOBNAME).zip
	# Include main tex, subdirectories, bib files, styles, and this Makefile
	zip -r source_$(JOBNAME).zip $(FILENAME).tex $(TEX_DIR)/ figures/ utils/ *.bib *.cls *.sty *.bst Makefile
	@echo "Zip archive created successfully!"

