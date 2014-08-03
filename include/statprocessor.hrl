-record(statprocessor, {
          name, % string
          version, % integer
          low_func, % fun(Map, Accum)
          low_init, % initial value
          high_func, % fun(Init, ResultsList)
          high_init % initial value
         }).
