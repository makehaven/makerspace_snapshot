(function ($, Drupal) {
  'use strict';

  Drupal.behaviors.makerspaceSnapshotDelete = {
    attach: function (context, settings) {
      $('.snapshot-delete-button', context).once('makerspace-snapshot-delete').on('click', function (e) {
        e.preventDefault();
        var snapshotId = $(this).data('snapshot-id');
        var url = '/admin/config/makerspace/snapshot/delete/' + snapshotId;

        $.ajax({
          url: url,
          type: 'POST',
          dataType: 'json',
          success: function (response) {
            if (response.success) {
              $('tr[data-snapshot-id="' + snapshotId + '"]').fadeOut(300, function () {
                $(this).remove();
                // Add a success message to the page
                $('.region-header').once('makerspace-snapshot-messages').append('<div class="messages messages--status">' + response.message + '</div>');

              });
            } else {
              // Add an error message to the page
              $('.region-header').once('makerspace-snapshot-messages').append('<div class="messages messages--error">' + response.message + '</div>');
            }
          }
        });
      });
    }
  };
})(jQuery, Drupal);
