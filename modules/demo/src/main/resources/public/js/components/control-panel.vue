<template>
  <div class="ControlPanel">
    <h1>Chord Demo</h1>
    <div class="operations">
      <button @click="addNode">Add Node</button>
    </div>
    <div class="meta">
      <div>Most recent node ID: {{mostRecentId}}</div>
      <div>Seed node ID: {{seedId}}</div>
    </div>
  </div>
</template>

<script>
module.exports = {
  data() {
    return {
      mostRecentId: null,
      seedId: null
    };
  },
  methods: {
    addNode() {
      axios.post('/nodes' + (this.seedId ? `?seed_id=${this.seedId}` : '')).then((response) => {
        this.mostRecentId = response.data.nodeId;
        if (this.seedId === null) {
          this.seedId = response.data.nodeId;
        }
      });
    }
  }
};
</script>

<style>
.ControlPanel {
  background-color: #f2f2f2;
  min-height: 100%;
  overflow-x: hidden;
  overflow-y: auto;
  padding: 0 0.9em 0 1.2em;
}

.ControlPanel > h1 {
  border-bottom: 1px solid #aaa;
  font-size: 22px;
  margin: 1.2em 0 1em 0;
  padding-bottom: 0.2em;
  width: 8.9em;
}

.ControlPanel > .operations {
  margin-left: 0.1em;
}

.ControlPanel > .meta {
  background: white;
  border: 1px solid #ccc;
  border-radius: 3px;
  margin-left: 0.1em;
  margin-right: 25px;
  margin-top: 20px;
}

.ControlPanel > .meta > div {
  margin: 10px;
}
</style>
