# I should ...

* generalize io somehow to make read_file go away for some construction
  like open_file >> read_handle
* implement io.write_file as brother for read_file
* implement producers and consumers for files 
* implement a combinator that applies a -> b to the n-th entry in a nested
  product.

# I could ...

* find something more convenient than _any = Type.get() for type
  vars
    * use literals like "x" for type vars
* implement sum types
* redo the TypeEngine. It is quite messy and somehow ad hoc.
* implement a way to vertically combine stream parts.
* create test for the environment on stream parts.
* implement recursive types to implement stuff like a json dict
* implement a stream type
