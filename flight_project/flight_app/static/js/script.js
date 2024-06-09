document.addEventListener("DOMContentLoaded", function() {
    const sections = document.querySelectorAll('.section');
    let currentIndex = 0;
    let isScrolling = false;

    function scrollToSection(index) {
        isScrolling = true;
        sections[index].scrollIntoView({ behavior: 'smooth' });
        setTimeout(() => {
            isScrolling = false;
        }, 1000); // Adjust timeout based on your scrolling duration
    }

    function handleScroll(event) {
        if (isScrolling) return;

        if (event.deltaY > 0 && currentIndex < sections.length - 1) {
            currentIndex++;
        } else if (event.deltaY < 0 && currentIndex > 0) {
            currentIndex--;
        }
        scrollToSection(currentIndex);
    }

    function handleKeyDown(event) {
        if (isScrolling) return;

        if (event.key === 'ArrowDown' && currentIndex < sections.length - 1) {
            currentIndex++;
        } else if (event.key === 'ArrowUp' && currentIndex > 0) {
            currentIndex--;
        }
        scrollToSection(currentIndex);
    }

    function handleMenuClick(event) {
        if (isScrolling) return;

        event.preventDefault();
        const targetId = event.target.getAttribute('href').substring(1);
        const targetIndex = Array.from(sections).findIndex(section => section.id === targetId);
        if (targetIndex !== -1) {
            currentIndex = targetIndex;
            scrollToSection(currentIndex);
        }
    }

    window.addEventListener('wheel', handleScroll);
    window.addEventListener('keydown', handleKeyDown);
    
    const menuLinks = document.querySelectorAll('.fixed-menu a');
    menuLinks.forEach(link => link.addEventListener('click', handleMenuClick));
    
});
