<template>
  <div class="ChordRing" ref="el"></div>
</template>

<script>
module.exports = {
  mounted() {
    const wsUri = "ws://127.0.0.1:4567/";
    const nodesUri = "http://127.0.0.1:4567/nodes";
    const w = 600;
    const h = 600;
    const radius = Math.min(w, h) / 2;

    // With a key-space that wraps back around to zero from 64, each node will
    // represent a ~5.625 degree arc on the ring
    const keyspaceModulus = 64;
    const keyspaceFactor = 360 / keyspaceModulus;

    // Chrome 15 bug: <http://code.google.com/p/chromium/issues/detail?id=98951>
    const div = d3.select(this.$refs.el).append("div")
        .style("width", w + "px")
        .style("height", w + "px")
        .style("margin", "0 auto")
        .style("-webkit-backface-visibility", "hidden");

    const svg = div.append("svg:svg")
        .attr("width", w)
        .attr("height", w)
        .append("svg:g")
        .attr("transform", "translate(" + radius + "," + radius + ")");

    // Draw outer Chord ring
    const arc = d3.svg.arc();
    svg.append("svg:path")
        .attr("class", "arc ring")
        .attr("d", arc.outerRadius(radius).innerRadius(0).startAngle(0).endAngle(2 * Math.PI));

    // Radial line generator that distributes nodes evenly based on the size of the key-space
    const line = d3.svg.line.radial()
        .interpolate("bundle")
        .tension(0.6)
        .radius(function (d) {
          return d.radius;
        })
        .angle(function (d) {
          return d.value * keyspaceFactor / 180 * Math.PI;
        });

    const bundle = d3.layout.bundle();
    const hierarchy = d3.layout.hierarchy();

    function createNodeOrUpdateSuccessor(svg, json) {
      // Assume node is active unless 'active' property has a falsey value
      const active = !json.hasOwnProperty('active') || json.active;

      // Update node if it already exists; otherwise create a new node
      let selection = svg.selectAll(".node-" + json.nodeId);
      if (selection.size() > 0) {
        selection.classed("inactive", !active);

      } else {
        // Find position of node on ring
        const x = radius * Math.cos((json.nodeId * keyspaceFactor - 90) / 180 * Math.PI);
        const y = radius * Math.sin((json.nodeId * keyspaceFactor - 90) / 180 * Math.PI);

        // Create new path
        svg.append("svg:path")
            .attr("class", "arc node node-" + json.nodeId + (active ? "" : " inactive"))
            .attr("d", arc.outerRadius(5).innerRadius(0).startAngle(0).endAngle(2 * Math.PI))
            .attr("transform", "translate(" + x + "," + y + ")");
      }

      // Update successor links for active nodes only
      if (active) {
        // Generate successor links for the current node
        const root = {parent: null, radius: 0};
        const source = {value: json.nodeId, radius: radius, parent: root};
        const target = {value: json.successorId, radius: radius, parent: root};
        root.children = [source, target];
        hierarchy(root);
        const splines = bundle([
          {
            source: source,
            target: target
          }
        ]);

        // Update successor link arc if it already exists; otherwise create a new one
        selection = svg.selectAll(".node-" + json.nodeId + "-successor");
        if (selection.size() > 0) {
          selection.attr("d", line(splines[0]));
        } else {
          svg.append("svg:path")
              .attr("class", "chord node-" + json.nodeId + "-successor")
              .attr("d", line(splines[0]));
        }
      }

      svg.selectAll(".node").moveToFront();
    }

    function markNodeAsInactive(svg, json) {
      svg.selectAll(".node-" + json.nodeId).classed("inactive", true);
      svg.selectAll(".node-" + json.nodeId + "-successor").remove();
    }

    d3.json(nodesUri, function (error, json) {
      if (error) {
        alert("Failed to retrieve existing node: " + error)
      } else {
        for (let i = 0; i < json.length; i++) {
          createNodeOrUpdateSuccessor(svg, json[i]);
        }
      }

      var websocket = new WebSocket(wsUri);

      websocket.onclose = function () {
        console.log("Connection closed");
      };

      websocket.onError = function (event) {
        console.log(event.data);
      };

      websocket.onmessage = function (event) {
        const json = JSON.parse(event.data);
        if (!json.hasOwnProperty('type')) {
          return;
        }

        switch (json.type) {
          case "SuccessorUpdated":
          case "NodeCreated":
            createNodeOrUpdateSuccessor(svg, json);
            break;
          case 'NodeDeleted':
            markNodeAsInactive(svg, json);
            break;
        }
      }
    });
  }
};
</script>

<style>
.ChordRing {
  background: #fff;
}
</style>
