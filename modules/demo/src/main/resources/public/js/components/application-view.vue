<template>
  <div class="ApplicationView">
    <div class="controlPanel">
      <control-panel></control-panel>
    </div>
    <div class="main">
      <chord-ring></chord-ring>
    </div>
  </div>
</template>

<script>
module.exports = {};
</script>

<style>
.ApplicationView {
  display: flex;
  height: 100%;
}

.ApplicationView > .controlPanel {
  min-height: 100%;
  min-width: 250px;
}

.ApplicationView > .main {
  border-left: 1px solid #aaa;
  box-shadow: -1px 0 4px rgba(0, 0, 0, 0.1);
  flex-grow: 1;
  height: 100%;
  left: 245px;
  max-height: 100%;
  overflow: auto;
}
</style>
