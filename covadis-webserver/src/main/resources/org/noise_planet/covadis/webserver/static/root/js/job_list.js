export default function initCheckboxToggle() {
    const cancelHeader = document.getElementById('cancelHeader');
    const deleteHeader = document.getElementById('deleteHeader');

    if (cancelHeader && deleteHeader) {
        cancelHeader.addEventListener('click', function () {
            const checkboxes = document.querySelectorAll('input[name="cancel"]:not([disabled])');
            for (let cb of checkboxes) {
                cb.checked = !cb.checked;
            }
        });

        deleteHeader.addEventListener('click', function () {
            const checkboxes = document.querySelectorAll('input[name="delete"]:not([disabled])');
            for (let cb of checkboxes) {
                cb.checked = !cb.checked;
            }
        });
    } else {
        console.error("One or more required elements were not found.");
    }
}
