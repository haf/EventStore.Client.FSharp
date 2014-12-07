fromAll()
  .when({
    $init: function() {
      return {
      }
    },
    "$any": function (state, event) {
      if (typeof state[event.streamId] === undefined
          || state[event.streamId] == null) {
        state[event.streamId] = [];
      };
      state[event.streamId][event.sequenceNumber] = event.data._name;
      return state;
    }
  })