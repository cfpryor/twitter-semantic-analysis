# twitter-semantic-analysis

# Stanford Core NLP setup:
curl https://nlp.stanford.edu/software/stanford-corenlp-full-2018-10-05.zip -O https://nlp.stanford.edu/software/stanford-english-corenlp-2018-10-05-models.jar -O
unzip stanford-corenlp-full-2018-10-05.zip
mv stanford-english-corenlp-2018-10-05-models.jar stanford-corenlp-full-2018-10-05
stanford-corenlp-full-2018-10-05.zip

# Running Stanford Core NLP Server:
java -mx5g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -timeout 1000
