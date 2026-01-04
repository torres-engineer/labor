MAIN=data_engineering
LATEXMK=latexmk
BIBER=biber
# PDF2PS=gs -dNOPAUSE -dBATCH -sDEVICE=ps2write -sOutputFile=$(MAIN).ps
PDF2PS=pdf2ps
# PLANTUML=plantuml
PLANTUML=java -jar /opt/plantuml/build/libs/plantuml-pdf-1.2025.10beta1.jar
SHELL:=/bin/sh

LATEXMKFLAGS=-xelatex -shell-escape -interaction=nonstopmode -file-line-error

UMLDIR=uml
UML_PUML=$(wildcard $(UMLDIR)/*.puml)
UML_PDFS=$(UML_PUML:.puml=.pdf)
UML_EPS=$(UML_PUML:.puml=.eps)

NOTEBOOKSDIR=../notebooks
RMD=$(NOTEBOOKSDIR)/analysis.Rmd
ANALYSIS_TEX=$(NOTEBOOKSDIR)/analysis.tex

.PHONY: all analysis pdf ps clean veryclean watch uml

all: pdf

analysis: $(ANALYSIS_TEX)

$(ANALYSIS_TEX): $(RMD)
	@echo ">>> Rendering RMarkdown analysis..."
	Rscript -e "rmarkdown::render('$(RMD)', output_format='pdf_document')"
	Rscript -e "rmarkdown::render('$(RMD)', output_format='latex_fragment')"
	ln -sf $(NOTEBOOKSDIR)/analysis_files analysis_files
	@# Convert the produced .tex into a standalone fragment if needed
	@# rmarkdown places output in notebooks/, so we're good.

pdf: $(MAIN).pdf

$(MAIN).pdf: $(MAIN).tex bibliography.bib analysis uml
	@echo ">>> Building PDF using latexmk (XeLaTeX + biber)..."
	$(LATEXMK) $(LATEXMKFLAGS) $(MAIN).tex
	@echo ">>> PDF ready: $(MAIN).pdf"

ps: $(MAIN).ps

$(MAIN).ps: $(MAIN).pdf
	@echo ">>> Converting PDF to PS ..."
	$(PDF2PS) $(MAIN).pdf $(MAIN).ps
	@echo ">>> PS ready: $(MAIN).ps"

uml: $(UML_PDFS)

$(UMLDIR)/%.pdf: $(UMLDIR)/%.puml
	@echo ">>> Generating $@ from $<"
	$(PLANTUML) -tpdf $<

$(UMLDIR)/%.eps: $(UMLDIR)/%.puml
	@echo ">>> Generating $@ from $<"
	$(PLANTUML) -teps $<

watch:
	@echo ">>> Watching source for changes (Ctrl+C to stop)..."
	$(LATEXMK) -pvc $(LATEXMKFLAGS) $(MAIN).tex

clean:
	@echo ">>> Cleaning auxiliary files..."
	$(LATEXMK) -c

veryclean: clean
	$(LATEXMK) -C
	rm -f $(UML_PDFS) $(UML_EPS)

