#app.py
#from flask import Flask, render_template, flash, request, redirect
from flask import Flask, render_template, request, redirect, abort, flash, url_for, jsonify
#from werkzeug.utils import secure_filename
from werkzeug.urls import url_parse
from werkzeug.utils import secure_filename
import os
#import magic
import urllib.request
from FlaskExample import xer_analyzer
app = Flask(__name__)

path='D:/XER-Output'
try: 
    os.mkdir(path) 
except OSError as error: 
    print(error)  


UPLOAD_FOLDER = path
 
app.secret_key = "Cairocoders-Ednalan"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
 
ALLOWED_EXTENSIONS = set([ 'xer'])
 
def allowed_file(filename):
 return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
  
@app.route('/')
def upload_form():
 return render_template('upload.html')
 
@app.route('/', methods=['POST'])
def upload_file():
 if request.method == 'POST':
        # check if the post request has the files part
  if 'files[]' not in request.files:
   flash('No file part')
   return redirect(request.url)
  files = request.files.getlist('files[]')
  xer_analyzer.ImportedFiles(files,path)

  for file in files:
   if file and allowed_file(file.filename):
    filename = secure_filename(file.filename)
    #file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
  flash('File(s) successfully uploaded')
  return redirect('/')

























if __name__ == '__main__':
 app.run(debug=True)