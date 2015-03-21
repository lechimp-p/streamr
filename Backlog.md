# I should ...

* find something more convenient than _any = Type.get() for type
  vars
* implement sum types
* generalize io somehow to make read_file go away for some construction
  like open_file >> read_handle
* implement io.write_file as brother for read_file
* implement producers and consumers for files 
* implement a combinator for map (a -> b) -> ([a] -> [b])

# I could ...

* redo the TypeEngine. It is quite messy and somehow ad hoc.
* implement a way to vertically combine stream parts.
* create test for the environment on stream parts.
* implement recursive types to implement stuff like a json dict
