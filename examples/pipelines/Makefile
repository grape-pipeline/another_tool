# tools
ALIGNER = ../tools/aligner
INTERLEAVE = ../tools/interleave
QUANTIFIER = ../tools/quantifier

# input data
INDEX = ../data/genome.gem
ANNOTATION = ../data/genome.gtf
READS = ../data/reads_1.fastq ../data/reads_2.fastq

all: counts.txt

input.fastq:
	$(INTERLEAVE) -o input.fastq $(READS)

alignment.map: input.fastq
	$(ALIGNER) --index $(INDEX) -i $@ -o alignment.map

counts.txt: alignment.map
	$(QUANTIFIER) --annotation $(ANNOTATION) -i alignment.map -o counts.txt

clean:
	rm -f alignment.map
	rm -f counts.txt
	rm -f input.fastq
