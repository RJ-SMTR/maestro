This folder contains a template on how to define a pipeline using 
the `pipeline.yaml` file.

How to setup the pipeline:

0. Copy the template to another folder.

1. Go to `pipeline.py`, change the function name and make sure that it is
loading the right `pipeline.yaml` file.

2. Go to `repositories.yaml` and point it to your newly created function.

3. Try out and see if it loads in dagit. 

4. Change the `pipeline.yaml` to fit your needs.

5. Go back to `pipeline.py` if you need to add hooks or modes.