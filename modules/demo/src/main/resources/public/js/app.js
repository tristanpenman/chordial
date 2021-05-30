d3.selection.prototype.moveToFront = function () {
  return this.each(function () {
    this.parentNode.appendChild(this);
  });
};

window.onload = function () {
  new Vue({
    el: document.getElementById('app'),
    template: `
      <chord-ring></chord-ring>
    `
  });
};
