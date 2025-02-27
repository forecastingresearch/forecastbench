all: pdf

pdf: poster.tex question_bank.tex timeline.tex leaderboard_llm_human.tex
	pdflatex poster.tex

png: poster.pdf
	magick -density 300 poster.pdf -resize 5120x2880 poster.png

thumbnail: png
	magick poster.png -resize 320x256 -background white -gravity center -extent 320x256 poster_thumbnail.png

upload-files: thumbnail

clean:
	rm -f poster.aux poster.log poster.nav poster.snm poster.toc poster.out poster.pdf poster.png poster_thumbnail.png
