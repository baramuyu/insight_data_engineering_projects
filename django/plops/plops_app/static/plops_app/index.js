"use strict";

var map;
function initMap() {
  console.log('init map');
  // The location of Uluru
  var insight = {lat: 47.602626, lng: -122.333980}; 
  // The map, centered at Uluru
  var map = new google.maps.Map(
      document.getElementById('map'), {zoom: 13, center: insight});
  // The marker, positioned at Uluru
  var marker = new google.maps.Marker({position: insight, map: map});

}