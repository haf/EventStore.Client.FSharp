fromCategory('programmers')
  .foreachStream()
  .when({
    $init: function() {
      return {
        seen: 0
      }
    },
    "$any": function (state, event) {
      state.seen += 1;
      return state;
    }
  })